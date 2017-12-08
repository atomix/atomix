/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.session.impl;

import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionListener;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

/**
 * Session manager.
 */
public class RaftSessionRegistry {
  private final Map<Long, RaftSession> sessions = new ConcurrentHashMap<>();
  private final Map<PrimitiveId, Set<SessionListener>> listeners = new ConcurrentHashMap<>();

  /**
   * Registers a session.
   */
  public void registerSession(RaftSession session) {
    if (sessions.putIfAbsent(session.sessionId().id(), session) == null) {
      Set<SessionListener> listeners = this.listeners.get(session.getService().serviceId());
      if (listeners != null) {
        listeners.forEach(l -> l.onOpen(session));
      }
    }
  }

  /**
   * Expires a session.
   */
  public void expireSession(SessionId sessionId) {
    RaftSession session = sessions.remove(sessionId.id());
    if (session != null) {
      Set<SessionListener> listeners = this.listeners.get(session.getService().serviceId());
      if (listeners != null) {
        listeners.forEach(l -> l.onExpire(session));
      }
      session.expire();
    }
  }

  /**
   * Closes a session.
   */
  public void closeSession(SessionId sessionId) {
    RaftSession session = sessions.remove(sessionId.id());
    if (session != null) {
      Set<SessionListener> listeners = this.listeners.get(session.getService().serviceId());
      if (listeners != null) {
        listeners.forEach(l -> l.onClose(session));
      }
      session.close();
    }
  }

  /**
   * Gets a session by session ID.
   *
   * @param sessionId The session ID.
   * @return The session or {@code null} if the session doesn't exist.
   */
  public RaftSession getSession(SessionId sessionId) {
    return getSession(sessionId.id());
  }

  /**
   * Gets a session by session ID.
   *
   * @param sessionId The session ID.
   * @return The session or {@code null} if the session doesn't exist.
   */
  public RaftSession getSession(long sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * Returns the collection of registered sessions.
   *
   * @return The collection of registered sessions.
   */
  public Collection<RaftSession> getSessions() {
    return sessions.values();
  }

  /**
   * Returns a set of sessions associated with the given service.
   *
   * @param primitiveId the service identifier
   * @return a collection of sessions associated with the given service
   */
  public Collection<RaftSession> getSessions(PrimitiveId primitiveId) {
    return sessions.values().stream()
        .filter(session -> session.getService().serviceId().equals(primitiveId))
        .filter(session -> session.getState().active())
        .collect(Collectors.toSet());
  }

  /**
   * Removes all sessions registered for the given service.
   *
   * @param primitiveId the service identifier
   */
  public void removeSessions(PrimitiveId primitiveId) {
    sessions.entrySet().removeIf(e -> e.getValue().getService().serviceId().equals(primitiveId));
  }

  /**
   * Adds a session listener.
   *
   * @param primitiveId     the service ID for which to listen to sessions
   * @param sessionListener the session listener
   */
  public void addListener(PrimitiveId primitiveId, SessionListener sessionListener) {
    Set<SessionListener> sessionListeners = listeners.computeIfAbsent(primitiveId, k -> new CopyOnWriteArraySet<>());
    sessionListeners.add(sessionListener);
  }

  /**
   * Removes a session listener.
   *
   * @param primitiveId     the service ID with which the listener is associated
   * @param sessionListener the session listener
   */
  public void removeListener(PrimitiveId primitiveId, SessionListener sessionListener) {
    Set<SessionListener> sessionListeners = listeners.get(primitiveId);
    if (sessionListeners != null) {
      sessionListeners.remove(sessionListener);
      if (sessionListeners.isEmpty()) {
        listeners.remove(primitiveId);
      }
    }
  }
}
