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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Listeners;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Raft session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Session {
  private final long id;
  private final int member;
  private final UUID connection;
  private boolean expired;
  private boolean closed;
  private final Listeners<Session> openListeners = new Listeners<>();
  private final Listeners<Session> closeListeners = new Listeners<>();

  protected Session(long id, int member, UUID connection) {
    this.id = id;
    this.member = member;
    this.connection = connection;
  }

  /**
   * Returns the session ID.
   *
   * @return The session ID.
   */
  public long id() {
    return id;
  }

  /**
   * Returns the session member ID.
   *
   * @return The session member ID.
   */
  public int member() {
    return member;
  }

  /**
   * Returns the session connection ID.
   *
   * @return The session connection ID.
   */
  public UUID connection() {
    return connection;
  }

  /**
   * Returns a boolean value indicating whether the session is open.
   *
   * @return Indicates whether the session is open.
   */
  public boolean isOpen() {
    return !closed;
  }

  /**
   * Sets an open listener on the session.
   *
   * @param listener The session open listener.
   * @return The listener context.
   */
  public ListenerContext<Session> onOpen(Listener<Session> listener) {
    return openListeners.add(listener);
  }

  /**
   * Publishes a message to the session.
   *
   * @param message The message to publish.
   * @return A completable future to be called once the message has been published.
   */
  public abstract CompletableFuture<Void> publish(Object message);

  /**
   * Sets a session receive listener.
   *
   * @param listener The session receive listener.
   * @return The listener context.
   */
  public abstract ListenerContext<?> onReceive(Listener<?> listener);

  /**
   * Closes the session.
   */
  protected void close() {
    closed = true;
    for (ListenerContext<Session> listener : closeListeners) {
      listener.accept(this);
    }
  }

  /**
   * Sets a session close listener.
   *
   * @param listener The session close listener.
   * @return The session.
   */
  public ListenerContext<Session> onClose(Listener<Session> listener) {
    ListenerContext<Session> context = closeListeners.add(listener);
    if (closed) {
      context.accept(this);
    }
    return context;
  }

  /**
   * Returns a boolean value indicating whether the session is closed.
   *
   * @return Indicates whether the session is closed.
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Expires the session.
   */
  protected void expire() {
    expired = true;
    close();
  }

  /**
   * Returns a boolean value indicating whether the session is expired.
   *
   * @return Indicates whether the session is expired.
   */
  public boolean isExpired() {
    return expired;
  }

  @Override
  public String toString() {
    return String.format("Session[id=%d]", id);
  }

}
