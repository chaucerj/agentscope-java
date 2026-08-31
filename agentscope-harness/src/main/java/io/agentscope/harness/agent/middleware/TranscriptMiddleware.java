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
package io.agentscope.harness.agent.middleware;

import io.agentscope.core.agent.Agent;
import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.message.Msg;
import io.agentscope.core.middleware.AgentInput;
import io.agentscope.core.state.AgentState;
import io.agentscope.harness.agent.HarnessAgent;
import io.agentscope.harness.agent.memory.session.SessionTranscriptWriter;
import io.agentscope.harness.agent.memory.session.SessionTree;
import io.agentscope.harness.agent.transcript.TranscriptStore;
import io.agentscope.harness.agent.workspace.WorkspaceManager;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Persists the conversation transcript at the end of every agent call.
 *
 * <p>Independent of memory flush: wired even when {@code disableMemoryHooks} is set, so session
 * history stays complete for Operate / {@code SessionSearchTool} / resumption.
 */
public class TranscriptMiddleware implements HarnessRuntimeMiddleware {

    private static final Logger log = LoggerFactory.getLogger(TranscriptMiddleware.class);

    /**
     * Upper bound for draining fire-and-forget transcript/session mirrors at the end of an append.
     * Mirrors the graceful-shutdown budget in {@code HarnessAgent#close()}; on timeout the append
     * still completes and the mirror stays asynchronous (pre-existing behavior).
     */
    private static final long MIRROR_QUIESCENCE_TIMEOUT_SECONDS = 5;

    private final WorkspaceManager workspaceManager;
    private final TranscriptStore transcriptStore;
    private final String tenant;

    public TranscriptMiddleware(WorkspaceManager workspaceManager) {
        this(workspaceManager, null, null);
    }

    public TranscriptMiddleware(
            WorkspaceManager workspaceManager, TranscriptStore transcriptStore, String tenant) {
        this.workspaceManager = workspaceManager;
        this.transcriptStore = transcriptStore;
        this.tenant = tenant;
    }

    @Override
    public Flux<AgentEvent> onAgent(
            Agent agent,
            RuntimeContext ctx,
            AgentInput input,
            Function<AgentInput, Flux<AgentEvent>> next) {
        final RuntimeContext rc = ctx != null ? ctx : RuntimeContext.empty();
        return next.apply(input)
                .concatWith(
                        Mono.defer(() -> appendTranscript(agent, rc))
                                .subscribeOn(Schedulers.boundedElastic())
                                .onErrorResume(
                                        e -> {
                                            log.warn(
                                                    "Transcript append failed: {}", e.getMessage());
                                            return Mono.empty();
                                        })
                                .then(Mono.<AgentEvent>empty()));
    }

    private Mono<Void> appendTranscript(Agent agent, RuntimeContext rc) {
        AgentState state = RuntimeContext.resolveAgentState(rc, agent);
        if (state == null) {
            return Mono.empty();
        }
        List<Msg> messages = state.getContext();
        if (messages.isEmpty()) {
            return Mono.empty();
        }
        String agentId = agent.getName();
        if (agent instanceof HarnessAgent harnessAgent) {
            String stable = harnessAgent.getAgentId();
            if (stable != null && !stable.isBlank()) {
                agentId = stable;
            }
        }
        String sessionId =
                rc.getSessionId() != null && !rc.getSessionId().isBlank()
                        ? rc.getSessionId()
                        : "default";
        SessionTranscriptWriter writer =
                new SessionTranscriptWriter(workspaceManager, transcriptStore, tenant);
        final String key = agentId;
        return Mono.fromRunnable(() -> writer.appendMessages(rc, messages, key, sessionId))
                // appendMessages schedules the transcript segment mirror as fire-and-forget work
                // on the shared single-thread mirror executor. Drain it (bounded) before the
                // chain completes so a caller that observes call() completion cannot race the
                // mirror's workspace writes — e.g. a test deleting its temp workspace right
                // after block(), which the mirror would otherwise recreate from under it.
                .then(
                        Mono.fromRunnable(
                                        () -> {
                                            if (!SessionTree.awaitMirrorQuiescence(
                                                    MIRROR_QUIESCENCE_TIMEOUT_SECONDS,
                                                    TimeUnit.SECONDS)) {
                                                log.debug(
                                                        "Transcript mirror did not quiesce within"
                                                                + " {}s; leaving it asynchronous",
                                                        MIRROR_QUIESCENCE_TIMEOUT_SECONDS);
                                            }
                                        })
                                .then())
                .doOnSuccess(v -> log.debug("Transcript append completed"))
                .onErrorResume(
                        e -> {
                            log.warn("Transcript append failed: {}", e.getMessage());
                            return Mono.empty();
                        });
    }
}
