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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.SessionListener;
import net.kuujo.copycat.raft.protocol.Connection;
import net.kuujo.copycat.raft.protocol.PublishRequest;
import net.kuujo.copycat.raft.protocol.PublishResponse;
import net.kuujo.copycat.raft.protocol.Response;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Server session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerSession extends Session {
  private final int client;
  private Connection connection;
  private final Set<SessionListener> listeners = new HashSet<>();

  ServerSession(long id, int client) {
    super(id);
    this.client = client;
  }

  /**
   * Returns the session client ID.
   *
   * @return The session's client ID.
   */
  int client() {
    return client;
  }

  /**
   * Expires the session.
   */
  protected void expire() {
    super.expire();
  }

  /**
   * Sets the session connection.
   */
  ServerSession setConnection(Connection connection) {
    this.connection = connection;
    connection.handler(PublishRequest.class, this::handlePublish);
    return this;
  }

  @Override
  public CompletableFuture<Void> publish(Object message) {
    if (connection == null)
      return CompletableFuture.completedFuture(null);

    return connection.send(PublishRequest.builder()
      .withSession(id())
      .withMessage(message)
      .build())
      .thenApply(v -> null);
  }

  /**
   * Handles a publish request.
   *
   * @param request The publish request to handle.
   * @return A completable future to be completed with the publish response.
   */
  protected CompletableFuture<PublishResponse> handlePublish(PublishRequest request) {
    for (SessionListener listener : listeners) {
      listener.messageReceived(request.message());
    }

    return CompletableFuture.completedFuture(PublishResponse.builder()
      .withStatus(Response.Status.OK)
      .build());
  }

  @Override
  public Session addListener(SessionListener listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    listeners.add(listener);
    return this;
  }

  @Override
  public Session removeListener(SessionListener listener) {
    if (listener == null)
      throw new NullPointerException("listener cannot be null");
    listeners.remove(listener);
    return this;
  }

}
