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

import java.util.concurrent.CompletableFuture;

/**
 * Raft session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Session {
  private final long id;
  private boolean expired;
  private boolean closed;

  protected Session(long id) {
    this.id = id;
  }

  /**
   * Returns the member session ID.
   *
   * @return The member session ID.
   */
  public long id() {
    return id;
  }

  /**
   * Opens the session.
   */
  protected void open() {
    closed = false;
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
   * Adds a listener to the session.
   *
   * @param listener The session listener.
   * @return The session.
   */
  public abstract Session addListener(Listener<?> listener);

  /**
   * Removes a listener from the session.
   *
   * @param listener The session listener.
   * @return The session.
   */
  public abstract Session removeListener(Listener<?> listener);

  /**
   * Publishes a message to the session.
   *
   * @param message The message to publish.
   * @return A completable future to be called once the message has been published.
   */
  public abstract CompletableFuture<Void> publish(Object message);

  /**
   * Closes the session.
   */
  protected void close() {
    closed = true;
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
