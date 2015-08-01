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
package net.kuujo.copycat.raft.server.state;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Listeners;
import net.kuujo.copycat.io.transport.Connection;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.UnknownSessionException;
import net.kuujo.copycat.raft.protocol.PublishRequest;
import net.kuujo.copycat.raft.protocol.PublishResponse;
import net.kuujo.copycat.raft.protocol.Response;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Raft session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerSession implements Session {
  protected final Listeners<Object> listeners = new Listeners<>();
  private final long id;
  private final UUID connectionId;
  private Connection connection;
  private long index;
  private long version;
  private long timestamp;
  private final Map<Long, List<Runnable>> queries = new HashMap<>();
  private final Map<Long, Object> responses = new HashMap<>();
  private boolean expired;
  private boolean closed;
  private final Listeners<Session> openListeners = new Listeners<>();
  private final Listeners<Session> closeListeners = new Listeners<>();

  ServerSession(long id, UUID connectionId) {
    if (connectionId == null)
      throw new NullPointerException("connection cannot be null");

    this.id = id;
    this.index = id;
    this.connectionId = connectionId;
  }

  @Override
  public long id() {
    return id;
  }

  /**
   * Returns the server connection ID.
   *
   * @return The server connection ID.
   */
  UUID connection() {
    return connectionId;
  }

  /**
   * Returns the session index.
   *
   * @return The session index.
   */
  long getIndex() {
    return index;
  }

  /**
   * Sets the session index.
   *
   * @param index The session index.
   * @return The server session.
   */
  ServerSession setIndex(long index) {
    this.index = index;
    return this;
  }

  /**
   * Returns the session version.
   *
   * @return The session version.
   */
  long getVersion() {
    return version;
  }

  /**
   * Sets the session version.
   *
   * @param version The session version.
   * @return The server session.
   */
  ServerSession setVersion(long version) {
    if (version > this.version) {
      responses.remove(this.version);

      for (long i = this.version + 1; i < version; i++) {
        List<Runnable> queries = this.queries.remove(i);
        if (queries != null) {
          for (Runnable query : queries) {
            query.run();
          }
        }
        responses.remove(i);
      }

      List<Runnable> queries = this.queries.remove(version);
      if (queries != null) {
        for (Runnable query : queries) {
          query.run();
        }
      }
      this.version = version;
    }
    return this;
  }

  /**
   * Returns the session timestamp.
   *
   * @return The session timestamp.
   */
  long getTimestamp() {
    return timestamp;
  }

  /**
   * Sets the session timestamp.
   *
   * @param timestamp The session timestamp.
   * @return The server session.
   */
  ServerSession setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * Registers a session query.
   *
   * @param version The session version.
   * @param query The session query.
   * @return The server session.
   */
  ServerSession registerQuery(long version, Runnable query) {
    List<Runnable> queries = this.queries.computeIfAbsent(version, v -> new ArrayList<>());
    queries.add(query);
    return this;
  }

  /**
   * Registers a session response.
   *
   * @param version The response version.
   * @param response The response.
   * @return The server session.
   */
  ServerSession registerResponse(long version, Object response) {
    responses.put(version, response);
    return this;
  }

  /**
   * Returns a boolean value indicating whether the session has a response for the given version.
   *
   * @param version The response version.
   * @return Indicates whether the session has a response for the given version.
   */
  boolean hasResponse(long version) {
    return responses.containsKey(version);
  }

  /**
   * Returns the session response for the given version.
   *
   * @param version The response version.
   * @return The response.
   */
  Object getResponse(long version) {
    return responses.get(version);
  }

  /**
   * Sets the session connection.
   */
  ServerSession setConnection(Connection connection) {
    this.connection = connection;
    if (connection != null) {
      if (!connection.id().equals(connectionId)) {
        throw new IllegalArgumentException("connection must match session connection ID");
      }
      connection.handler(PublishRequest.class, this::handlePublish);
    }
    return this;
  }

  @Override
  public CompletableFuture<Void> publish(Object message) {
    if (connection == null)
      return Futures.exceptionalFuture(new UnknownSessionException("connection lost"));

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
  @SuppressWarnings("unchecked")
  protected CompletableFuture<PublishResponse> handlePublish(PublishRequest request) {
    for (ListenerContext listener : listeners) {
      listener.accept(request.message());
    }

    return CompletableFuture.completedFuture(PublishResponse.builder()
      .withStatus(Response.Status.OK)
      .build());
  }

  @Override
  public boolean isOpen() {
    return !closed;
  }

  @Override
  public ListenerContext<Session> onOpen(Listener<Session> listener) {
    return openListeners.add(listener);
  }

  @Override
  @SuppressWarnings("unchecked")
  public ListenerContext<?> onReceive(Listener listener) {
    return listeners.add(listener);
  }

  /**
   * Closes the session.
   */
  void close() {
    closed = true;
    for (ListenerContext<Session> listener : closeListeners) {
      listener.accept(this);
    }
  }

  @Override
  public ListenerContext<Session> onClose(Listener<Session> listener) {
    ListenerContext<Session> context = closeListeners.add(listener);
    if (closed) {
      context.accept(this);
    }
    return context;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  /**
   * Expires the session.
   */
  void expire() {
    closed = true;
    expired = true;
    for (ListenerContext<Session> listener : closeListeners) {
      listener.accept(this);
    }
  }

  @Override
  public boolean isExpired() {
    return expired;
  }

  @Override
  public String toString() {
    return String.format("Session[id=%d]", id);
  }

}
