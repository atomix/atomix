/*
 * Copyright 2017-present Open Networking Laboratory
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

import com.google.common.collect.Sets;
import io.atomix.event.Event;
import io.atomix.event.EventListener;
import io.atomix.logging.Logger;
import io.atomix.logging.LoggerFactory;
import io.atomix.protocols.raft.protocol.PublishRequest;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.ResetRequest;
import io.atomix.serializer.Serializer;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client session message listener.
 */
final class RaftProxyListener {
  private static final Logger LOG = LoggerFactory.getLogger(RaftProxyListener.class);

  private final RaftClientProtocol protocol;
  private final RaftProxyState state;
  private final Set<EventListener> listeners = Sets.newLinkedHashSet();
  private final RaftProxySequencer sequencer;
  private final Serializer serializer;
  private final Executor executor;

  public RaftProxyListener(RaftClientProtocol protocol, RaftProxyState state, RaftProxySequencer sequencer, Serializer serializer, Executor executor) {
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.state = checkNotNull(state, "state cannot be null");
    this.sequencer = checkNotNull(sequencer, "sequencer cannot be null");
    this.serializer = checkNotNull(serializer, "serializer cannot be null");
    this.executor = checkNotNull(executor, "executor cannot be null");
    protocol.registerPublishListener(state.getSessionId(), this::handlePublish, executor);
  }

  /**
   * Adds an event listener to the session.
   *
   * @param listener the event listener callback
   */
  public void addEventListener(EventListener listener) {
    executor.execute(() -> listeners.add(listener));
  }

  /**
   * Removes an event listener from the session.
   *
   * @param listener the event listener callback
   */
  public void removeEventListener(EventListener listener) {
    executor.execute(() -> listeners.remove(listener));
  }

  /**
   * Handles a publish request.
   *
   * @param request The publish request to handle.
   */
  @SuppressWarnings("unchecked")
  private void handlePublish(PublishRequest request) {
    LOG.trace("{} - Received {}", state.getSessionId(), request);

    // If the request is for another session ID, this may be a session that was previously opened
    // for this client.
    if (request.session() != state.getSessionId()) {
      LOG.trace("{} - Inconsistent session ID: {}", state.getSessionId(), request.session());
      return;
    }

    // Store eventIndex in a local variable to prevent multiple volatile reads.
    long eventIndex = state.getEventIndex();

    // If the request event index has already been processed, return.
    if (request.eventIndex() <= eventIndex) {
      LOG.trace("{} - Duplicate event index {}", state.getSessionId(), request.eventIndex());
      return;
    }

    // If the request's previous event index doesn't equal the previous received event index,
    // respond with an undefined error and the last index received. This will cause the cluster
    // to resend events starting at eventIndex + 1.
    if (request.previousIndex() != eventIndex) {
      LOG.trace("{} - Inconsistent event index: {}", state.getSessionId(), request.previousIndex());
      ResetRequest resetRequest = ResetRequest.builder()
          .withSession(state.getSessionId())
          .withIndex(eventIndex)
          .build();
      protocol.reset(resetRequest);
      return;
    }

    // Store the event index. This will be used to verify that events are received in sequential order.
    state.setEventIndex(request.eventIndex());

    sequencer.sequenceEvent(request, () -> {
      for (byte[] bytes : request.events()) {
        Event event = serializer.decode(bytes);
        for (EventListener listener : listeners) {
          listener.onEvent(event);
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
