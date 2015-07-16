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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sessions.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Sessions {
  private final Map<Long, Session> sessions = new ConcurrentHashMap<>();
  private final Listeners<Session> listeners = new Listeners<>();

  /**
   * Adds a session to the sessions map.
   */
  protected void addSession(Session session) {
    sessions.put(session.id(), session);
    listeners.forEach(l -> l.accept(session));
  }

  /**
   * Removes a session from the session map.
   */
  protected void removeSession(Session session) {
    sessions.remove(session.id());
    session.close();
  }

  /**
   * Returns a session by ID.
   *
   * @param sessionId The session ID.
   * @return The session.
   */
  public Session session(long sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * Returns a collection of open sessions.
   *
   * @return A collection of open sessions.
   */
  public Collection<Session> sessions() {
    return sessions.values();
  }

  /**
   * Sets a session listener.
   *
   * @param listener The session listener.
   * @return The registered listener.
   */
  public ListenerContext<Session> onSession(Listener<Session> listener) {
    return listeners.add(listener);
  }

}
