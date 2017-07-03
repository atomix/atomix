/*
 * Copyright 2017-present Open Networking Laboratory
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

import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessionListener;
import io.atomix.protocols.raft.session.RaftSessions;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.session.impl.RaftSessionManager;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * State machine sessions.
 */
class DefaultServiceSessions implements RaftSessions {
  private final RaftSessionManager sessionManager;
  private final Map<Long, RaftSessionContext> sessions = new ConcurrentHashMap<>();
  private final Set<RaftSessionListener> listeners = new HashSet<>();

  DefaultServiceSessions(RaftSessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  /**
   * Adds a session to the sessions list.
   *
   * @param session The session to add.
   */
  void add(RaftSessionContext session) {
    sessions.put(session.sessionId().id(), session);
    sessionManager.registerSession(session);
  }

  /**
   * Removes a session from the sessions list.
   *
   * @param session The session to remove.
   */
  void remove(RaftSessionContext session) {
    sessions.remove(session.sessionId().id());
    sessionManager.unregisterSession(session.sessionId().id());
  }

  /**
   * Clears the sessions.
   */
  void clear() {
    sessions.values().forEach(session -> sessionManager.unregisterSession(session.sessionId().id()));
    sessions.clear();
  }

  /**
   * Returns the session contexts.
   *
   * @return The session contexts.
   */
  Collection<RaftSessionContext> getSessions() {
    return sessions.values();
  }

  @Override
  public RaftSession getSession(long sessionId) {
    return sessions.get(sessionId);
  }

  @Override
  public RaftSessions addListener(RaftSessionListener listener) {
    listeners.add(listener);
    return this;
  }

  @Override
  public RaftSessions removeListener(RaftSessionListener listener) {
    listeners.remove(listener);
    return this;
  }

  /**
   * Returns the session listeners.
   *
   * @return The session listeners.
   */
  Collection<RaftSessionListener> getListeners() {
    return listeners;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<RaftSession> iterator() {
    return (Iterator) sessions.values().iterator();
  }
}