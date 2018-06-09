/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.proxy.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.client.SessionClient;
import io.atomix.protocols.raft.protocol.PublishRequest;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.ResetRequest;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client session message listener.
 */
final class RaftProxyListener {
  private final Logger log;
  private final RaftClientProtocol protocol;
  private final MemberSelector memberSelector;
  private final RaftProxyState state;
  private final Map<EventType, Set<Consumer<PrimitiveEvent>>> eventListeners = Maps.newHashMap();
  private final RaftProxySequencer sequencer;
  private final Executor executor;

  public RaftProxyListener(RaftClientProtocol protocol, MemberSelector memberSelector, RaftProxyState state, RaftProxySequencer sequencer, Executor executor) {
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.memberSelector = checkNotNull(memberSelector, "nodeSelector cannot be null");
    this.state = checkNotNull(state, "state cannot be null");
    this.sequencer = checkNotNull(sequencer, "sequencer cannot be null");
    this.executor = checkNotNull(executor, "executor cannot be null");
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(SessionClient.class)
        .addValue(state.getSessionId())
        .add("type", state.getPrimitiveType())
        .add("name", state.getPrimitiveName())
        .build());
    protocol.registerPublishListener(state.getSessionId(), this::handlePublish, executor);
  }

  /**
   * Adds an event listener to the session.
   *
   * @param listener the event listener callback
   */
  public void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    executor.execute(() -> eventListeners.computeIfAbsent(eventType.canonicalize(), e -> Sets.newLinkedHashSet()).add(listener));
  }

  /**
   * Removes an event listener from the session.
   *
   * @param listener the event listener callback
   */
  public void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    executor.execute(() -> eventListeners.computeIfAbsent(eventType.canonicalize(), e -> Sets.newLinkedHashSet()).remove(listener));
  }

  /**
   * Handles a publish request.
   *
   * @param request The publish request to handle.
   */
  @SuppressWarnings("unchecked")
  private void handlePublish(PublishRequest request) {
    log.trace("Received {}", request);

    // If the request is for another session ID, this may be a session that was previously opened
    // for this client.
    if (request.session() != state.getSessionId().id()) {
      log.trace("Inconsistent session ID: {}", request.session());
      return;
    }

    // Store eventIndex in a local variable to prevent multiple volatile reads.
    long eventIndex = state.getEventIndex();

    // If the request event index has already been processed, return.
    if (request.eventIndex() <= eventIndex) {
      log.trace("Duplicate event index {}", request.eventIndex());
      return;
    }

    // If the request's previous event index doesn't equal the previous received event index,
    // respond with an undefined error and the last index received. This will cause the cluster
    // to resend events starting at eventIndex + 1.
    if (request.previousIndex() != eventIndex) {
      log.trace("Inconsistent event index: {}", request.previousIndex());
      ResetRequest resetRequest = ResetRequest.builder()
          .withSession(state.getSessionId().id())
          .withIndex(eventIndex)
          .build();
      log.trace("Sending {}", resetRequest);
      protocol.reset(memberSelector.members(), resetRequest);
      return;
    }

    // Store the event index. This will be used to verify that events are received in sequential order.
    state.setEventIndex(request.eventIndex());

    sequencer.sequenceEvent(request, () -> {
      for (PrimitiveEvent event : request.events()) {
        Set<Consumer<PrimitiveEvent>> listeners = eventListeners.get(event.type());
        if (listeners != null) {
          for (Consumer<PrimitiveEvent> listener : listeners) {
            listener.accept(event);
          }
        }
      }
    });
  }

  /**
   * Closes the session event listener.
   *
   * @return A completable future to be completed once the listener is closed.
   */
  public CompletableFuture<Void> close() {
    protocol.unregisterPublishListener(state.getSessionId());
    return CompletableFuture.completedFuture(null);
  }
}
