// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.session;

import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.session.SessionId;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Session manager.
 */
public class RaftSessionRegistry {
  private final Map<Long, RaftSession> sessions = new ConcurrentHashMap<>();

  /**
   * Adds a session.
   */
  public RaftSession addSession(RaftSession session) {
    RaftSession existingSession = sessions.putIfAbsent(session.sessionId().id(), session);
    return existingSession != null ? existingSession : session;
  }

  /**
   * Closes a session.
   */
  public RaftSession removeSession(SessionId sessionId) {
    return sessions.remove(sessionId.id());
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
}
