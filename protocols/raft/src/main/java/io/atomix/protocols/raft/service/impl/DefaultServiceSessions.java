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
package io.atomix.protocols.raft.service.impl;

import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionListener;
import io.atomix.primitive.session.Sessions;
import io.atomix.protocols.raft.session.impl.RaftSession;
import io.atomix.protocols.raft.session.impl.RaftSessionRegistry;

import java.util.Collection;
import java.util.Iterator;

/**
 * State machine sessions.
 */
class DefaultServiceSessions implements Sessions {
  private final PrimitiveId primitiveId;
  private final RaftSessionRegistry sessionManager;

  public DefaultServiceSessions(PrimitiveId primitiveId, RaftSessionRegistry sessionManager) {
    this.primitiveId = primitiveId;
    this.sessionManager = sessionManager;
  }

  /**
   * Adds a session to the sessions list.
   *
   * @param session The session to add.
   */
  void openSession(RaftSession session) {
    sessionManager.registerSession(session);
  }

  /**
   * Expires and removes a session from the sessions list.
   *
   * @param session The session to remove.
   */
  void expireSession(RaftSession session) {
    sessionManager.expireSession(session.sessionId());
  }

  /**
   * Closes and removes a session from the sessions list.
   *
   * @param session The session to remove.
   */
  void closeSession(RaftSession session) {
    sessionManager.closeSession(session.sessionId());
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
    return sessionManager.getSession(sessionId);
  }

  @Override
  public Sessions addListener(SessionListener listener) {
    sessionManager.addListener(primitiveId, listener);
    return this;
  }

  @Override
  public Sessions removeListener(SessionListener listener) {
    sessionManager.removeListener(primitiveId, listener);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<Session> iterator() {
    return (Iterator) getSessions().iterator();
  }
}