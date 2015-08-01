/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft.client.state;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Listeners;
import net.kuujo.copycat.io.transport.Connection;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.protocol.PublishRequest;
import net.kuujo.copycat.raft.protocol.PublishResponse;
import net.kuujo.copycat.raft.protocol.Response;
import net.kuujo.copycat.util.concurrent.Context;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Client session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ClientSession implements Session {

  /**
   * Session state.
   */
  private static enum State {
    OPEN,
    CLOSED,
    EXPIRED
  }

  private final Listeners<Session> openListeners = new Listeners<>();
  private final Listeners<Session> closeListeners = new Listeners<>();
  private final Listeners<Object> receiveListeners = new Listeners<>();
  private final Context context;
  private volatile State state = State.CLOSED;
  private volatile long id;
  private volatile UUID connectionId;
  private long request;
  private long version;

  public ClientSession(Context context) {
    this.context = context;
  }

  @Override
  public long id() {
    return id;
  }

  /**
   * Sets the session connection.
   */
  void connect(Connection connection) {
    connection.handler(PublishRequest.class, this::handlePublish);
  }

  /**
   * Returns the next session request sequence number.
   */
  long nextRequest() {
    return ++request;
  }

  /**
   * Sets the session version number.
   */
  void setVersion(long version) {
    this.version = Math.max(this.version, version);
  }

  /**
   * Returns the session version number.
   */
  long getVersion() {
    return version;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ListenerContext<?> onReceive(Listener listener) {
    return receiveListeners.add(listener);
  }

  @Override
  public CompletableFuture<Void> publish(Object message) {
    return CompletableFuture.runAsync(() -> {
      for (Listener<Object> listener : receiveListeners) {
        listener.accept(message);
      }
    }, context);
  }

  /**
   * Handles a publish request.
   *
   * @param request The publish request to handle.
   * @return A completable future to be completed with the publish response.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<PublishResponse> handlePublish(PublishRequest request) {
    for (Listener listener : receiveListeners) {
      listener.accept(request.message());
    }

    return CompletableFuture.completedFuture(PublishResponse.builder()
      .withStatus(Response.Status.OK)
      .build());
  }

  /**
   * Opens the session.
   */
  void open(long id, UUID connectionId) {
    this.state = State.OPEN;
    this.id = id;
    this.connectionId = connectionId;

    for (Listener<Session> listener : openListeners) {
      listener.accept(this);
    }
  }

  @Override
  public boolean isOpen() {
    return state == State.OPEN;
  }

  @Override
  public ListenerContext<Session> onOpen(Listener<Session> listener) {
    return openListeners.add(listener);
  }

  /**
   * Closes the session.
   */
  void close() {
    state = State.CLOSED;
    triggerClose();
  }

  /**
   * Triggers close listeners.
   */
  private void triggerClose() {
    for (Listener<Session> listener : closeListeners) {
      listener.accept(this);
    }

    id = 0;
    connectionId = null;
  }

  @Override
  public ListenerContext<Session> onClose(Listener<Session> listener) {
    ListenerContext<Session> context = closeListeners.add(listener);
    if (isClosed()) {
      context.accept(this);
    }
    return context;
  }

  @Override
  public boolean isClosed() {
    return state == State.CLOSED || state == State.EXPIRED;
  }

  /**
   * Expires the session.
   */
  void expire() {
    state = State.EXPIRED;
    triggerClose();
  }

  @Override
  public boolean isExpired() {
    return state == State.EXPIRED;
  }

}
