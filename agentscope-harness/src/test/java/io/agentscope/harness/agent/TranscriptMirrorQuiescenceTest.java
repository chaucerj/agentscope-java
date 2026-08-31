/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.harness.agent;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.Model;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

/**
 * Reproduces the chronic CI @TempDir flake where a test passes but JUnit fails to delete its temp
 * workspace with {@code DirectoryNotEmptyException} on a namespace directory (e.g. {@code bob} or a
 * session id) that reappears during teardown.
 *
 * <p>Cause: {@code SessionTree#flush()} schedules the transcript segment mirror as fire-and-forget
 * work on the shared single-thread mirror executor. When that executor's queue is backed up (as in
 * a full CI JVM), the mirror's {@code Files.createDirectories} runs after the test method returns —
 * recreating the namespace directory chain inside a workspace JUnit is already deleting.
 *
 * <p>Fix under test: {@code TranscriptMiddleware} drains the mirror queue (bounded) before the
 * transcript append chain completes, so observing {@code call()} completion implies the mirror's
 * workspace writes have landed.
 */
class TranscriptMirrorQuiescenceTest {

    @TempDir Path stateHome;

    @TempDir Path workspace;

    private String previousStateHome;

    @BeforeEach
    void overrideStateHome() {
        previousStateHome = System.getProperty("agentscope.state.home");
        System.setProperty("agentscope.state.home", stateHome.toString());
    }

    @AfterEach
    void restoreStateHome() {
        if (previousStateHome != null) {
            System.setProperty("agentscope.state.home", previousStateHome);
        } else {
            System.clearProperty("agentscope.state.home");
        }
    }

    @Test
    void callCompletionImpliesTranscriptMirrorLanded_noPostTeardownRecreate() throws Exception {
        Files.writeString(workspace.resolve("AGENTS.md"), "# Test\n");

        // Stall the shared single-thread mirror executor, exactly as a backed-up CI JVM does.
        ExecutorService mirrorExecutor = mirrorExecutor();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch blockerStarted = new CountDownLatch(1);
        mirrorExecutor.execute(
                () -> {
                    blockerStarted.countDown();
                    try {
                        release.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
        assertTrue(blockerStarted.await(10, TimeUnit.SECONDS), "mirror blocker should start");

        String agentName = "mirror-" + UUID.randomUUID();
        HarnessAgent agent =
                HarnessAgent.builder()
                        .name(agentName)
                        .model(stubModel("done"))
                        .workspace(workspace)
                        .build();

        AtomicBoolean callDone = new AtomicBoolean(false);
        CountDownLatch callReturned = new CountDownLatch(1);
        Thread caller =
                new Thread(
                        () -> {
                            agent.call(
                                            userMsg("hi"),
                                            RuntimeContext.builder()
                                                    .userId("bob")
                                                    .sessionId("s1")
                                                    .build())
                                    .block();
                            callDone.set(true);
                            callReturned.countDown();
                        },
                        "mirror-quiescence-caller");
        caller.start();

        // While the mirror queue is stalled the call must NOT complete: the append chain drains
        // the queue before finishing. Pre-fix, the call returned immediately with the mirror
        // still pending.
        Thread.sleep(1500);
        assertFalse(callDone.get(), "call() should wait for the transcript mirror to drain");

        release.countDown();
        assertTrue(
                callReturned.await(10, TimeUnit.SECONDS), "call should finish once queue drains");

        // Simulate JUnit @TempDir teardown: delete everything under the workspace (children only,
        // so the assertions below can list the root).
        try (Stream<Path> walk = Files.walk(workspace)) {
            walk.filter(p -> !p.equals(workspace))
                    .sorted(Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    Files.deleteIfExists(p);
                                } catch (Exception ignored) {
                                    // best-effort, like JUnit
                                }
                            });
        }

        // The mirror already landed before call() returned, so nothing may recreate the
        // namespace directory after the wipe — that recreation is the CI flake signature.
        Thread.sleep(2000);
        Path namespaceDir = workspace.resolve("bob");
        assertFalse(
                Files.exists(namespaceDir),
                "transcript mirror must not recreate the namespace directory"
                        + " after the workspace was wiped (CI @TempDir flake)");

        caller.join(10_000);
    }

    /**
     * The mirror executor is an implementation detail of {@code SessionTree}; reach it via
     * reflection to deterministically stall it. Same classpath (non-modular surefire run), so
     * {@code setAccessible} is legal.
     */
    private static ExecutorService mirrorExecutor() throws Exception {
        Field f =
                Class.forName("io.agentscope.harness.agent.memory.session.SessionTree")
                        .getDeclaredField("MIRROR_EXECUTOR");
        f.setAccessible(true);
        return (ExecutorService) f.get(null);
    }

    private static Msg userMsg(String text) {
        return Msg.builder()
                .role(MsgRole.USER)
                .content(TextBlock.builder().text(text).build())
                .build();
    }

    private static Model stubModel(String text) {
        Model model = mock(Model.class);
        when(model.getModelName()).thenReturn("stub-model");
        ChatResponse chunk =
                new ChatResponse(
                        "stub-id",
                        List.of(TextBlock.builder().text(text).build()),
                        null,
                        Map.of(),
                        "stop");
        when(model.stream(anyList(), any(), any())).thenReturn(Flux.just(chunk));
        return model;
    }
}
