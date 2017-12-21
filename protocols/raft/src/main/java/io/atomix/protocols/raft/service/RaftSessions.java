/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.raft.service;

import com.google.common.collect.Sets;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionListener;
import io.atomix.primitive.session.Sessions;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessionRegistry;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * State machine sessions.
 */
class RaftSessions implements Sessions {
  private final PrimitiveId primitiveId;
  private final Set<SessionListener> listeners = Sets.newIdentityHashSet();
  private final RaftSessionRegistry sessionManager;

  public RaftSessions(PrimitiveId primitiveId, RaftSessionRegistry sessionManager) {
    this.primitiveId = primitiveId;
    this.sessionManager = sessionManager;
  }

  /**
   * Adds a session to the sessions set.
   *
   * @param session the session to add
   * @return the added session or an existing session if the session already exists
   */
  RaftSession addSession(RaftSession session) {
    return sessionManager.addSession(session);
  }

  /**
   * Adds a session to the sessions list.
   *
   * @param session The session to add.
   */
  void openSession(RaftSession session) {
    final RaftSession singletonSession = sessionManager.addSession(session);
    singletonSession.open();
    listeners.forEach(l -> l.onOpen(singletonSession));
  }

  /**
   * Expires and removes a session from the sessions list.
   *
   * @param session The session to remove.
   */
  void expireSession(RaftSession session) {
    final RaftSession singletonSession = sessionManager.removeSession(session.sessionId());
    if (singletonSession != null) {
      singletonSession.expire();
      listeners.forEach(l -> l.onExpire(singletonSession));
    }
  }

  /**
   * Closes and removes a session from the sessions list.
   *
   * @param session The session to remove.
   */
  void closeSession(RaftSession session) {
    final RaftSession singletonSession = sessionManager.removeSession(session.sessionId());
    if (singletonSession != null) {
      singletonSession.close();
      listeners.forEach(l -> l.onClose(singletonSession));
    }
  }

  /**
   * Returns the session contexts.
   *
   * @return The session contexts.
   */
  Collection<RaftSession> getSessions() {
    return sessionManager.getSessions(primitiveId);
  }

  @Override
  public Session getSession(long sessionId) {
    RaftSession session = sessionManager.getSession(sessionId);
    return session != null && session.getState().active() ? session : null;
  }

  @Override
  public Sessions addListener(SessionListener listener) {
    listeners.add(listener);
    return this;
  }

  @Override
  public Sessions removeListener(SessionListener listener) {
    listeners.remove(listener);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<Session> iterator() {
    return (Iterator) getSessions().iterator();
  }
}