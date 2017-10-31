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

import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.session.RaftSessionListener;
import io.atomix.protocols.raft.session.SessionId;

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
  private final Map<Long, RaftSessionContext> sessions = new ConcurrentHashMap<>();
  private final Map<ServiceId, Set<RaftSessionListener>> listeners = new ConcurrentHashMap<>();

  /**
   * Registers a session.
   */
  public void registerSession(RaftSessionContext session) {
    if (sessions.putIfAbsent(session.sessionId().id(), session) == null) {
      Set<RaftSessionListener> listeners = this.listeners.get(session.getService().serviceId());
      if (listeners != null) {
        listeners.forEach(l -> l.onOpen(session));
      }
    }
  }

  /**
   * Expires a session.
   */
  public void expireSession(SessionId sessionId) {
    RaftSessionContext session = sessions.remove(sessionId.id());
    if (session != null) {
      Set<RaftSessionListener> listeners = this.listeners.get(session.getService().serviceId());
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
    RaftSessionContext session = sessions.remove(sessionId.id());
    if (session != null) {
      Set<RaftSessionListener> listeners = this.listeners.get(session.getService().serviceId());
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
  public RaftSessionContext getSession(SessionId sessionId) {
    return getSession(sessionId.id());
  }

  /**
   * Gets a session by session ID.
   *
   * @param sessionId The session ID.
   * @return The session or {@code null} if the session doesn't exist.
   */
  public RaftSessionContext getSession(long sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * Returns the collection of registered sessions.
   *
   * @return The collection of registered sessions.
   */
  public Collection<RaftSessionContext> getSessions() {
    return sessions.values();
  }

  /**
   * Returns a set of sessions associated with the given service.
   *
   * @param serviceId the service identifier
   * @return a collection of sessions associated with the given service
   */
  public Collection<RaftSessionContext> getSessions(ServiceId serviceId) {
    return sessions.values().stream()
        .filter(session -> session.getService().serviceId().equals(serviceId))
        .collect(Collectors.toSet());
  }

  /**
   * Removes all sessions registered for the given service.
   *
   * @param serviceId the service identifier
   */
  public void removeSessions(ServiceId serviceId) {
    sessions.entrySet().removeIf(e -> e.getValue().getService().serviceId().equals(serviceId));
  }

  /**
   * Adds a session listener.
   *
   * @param serviceId the service ID for which to listen to sessions
   * @param sessionListener the session listener
   */
  public void addListener(ServiceId serviceId, RaftSessionListener sessionListener) {
    Set<RaftSessionListener> sessionListeners = listeners.computeIfAbsent(serviceId, k -> new CopyOnWriteArraySet<>());
    sessionListeners.add(sessionListener);
  }

  /**
   * Removes a session listener.
   *
   * @param serviceId the service ID with which the listener is associated
   * @param sessionListener the session listener
   */
  public void removeListener(ServiceId serviceId, RaftSessionListener sessionListener) {
    Set<RaftSessionListener> sessionListeners = listeners.get(serviceId);
    if (sessionListeners != null) {
      sessionListeners.remove(sessionListener);
      if (sessionListeners.isEmpty()) {
        listeners.remove(serviceId);
      }
    }
  }
}
