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

import java.util.HashSet;
import java.util.Set;

/**
 * Raft session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Session {
  private final long id;
  private boolean expired;
  private boolean closed;
  private final Set<SessionListener> listeners = new HashSet<>();

  public Session(long id) {
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
   * Returns a boolean value indicating whether the session is open.
   *
   * @return Indicates whether the session is open.
   */
  public boolean isOpen() {
    return !closed;
  }

  /**
   * Closes the session.
   */
  protected void close() {
    closed = true;
    listeners.forEach(l -> l.sessionClosed(this));
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
    listeners.forEach(l -> l.sessionExpired(this));
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

  /**
   * Adds a listener to the session.
   *
   * @param listener The session listener.
   * @return The session.
   */
  public Session addListener(SessionListener listener) {
    listeners.add(listener);
    return this;
  }

  /**
   * Removes a listener from the session.
   *
   * @param listener The session listener.
   * @return The session.
   */
  public Session removeListener(SessionListener listener) {
    listeners.remove(listener);
    return this;
  }

  @Override
  public String toString() {
    return String.format("Session[id=%d]", id);
  }

}
