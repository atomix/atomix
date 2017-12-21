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

import com.google.common.collect.Sets;
import io.atomix.protocols.raft.service.ServiceId;
import io.atomix.protocols.raft.session.RaftSession;
import io.atomix.protocols.raft.session.RaftSessionListener;
import io.atomix.protocols.raft.session.RaftSessions;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.session.impl.RaftSessionRegistry;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * State machine sessions.
 */
class DefaultServiceSessions implements RaftSessions {
  private final ServiceId serviceId;
  private final Set<RaftSessionListener> listeners = Sets.newIdentityHashSet();
  private final RaftSessionRegistry sessionManager;

  public DefaultServiceSessions(ServiceId serviceId, RaftSessionRegistry sessionManager) {
    this.serviceId = serviceId;
    this.sessionManager = sessionManager;
  }

  /**
   * Adds a session to the sessions set.
   *
   * @param session the session to add
   * @return the added session or an existing session if the session already exists
   */
  RaftSessionContext addSession(RaftSessionContext session) {
    return sessionManager.addSession(session);
  }

  /**
   * Adds a session to the sessions list.
   *
   * @param session The session to add.
   */
  void openSession(RaftSessionContext session) {
    final RaftSessionContext singletonSession = sessionManager.addSession(session);
    singletonSession.open();
    listeners.forEach(l -> l.onOpen(singletonSession));
  }

  /**
   * Expires and removes a session from the sessions list.
   *
   * @param session The session to remove.
   */
  void expireSession(RaftSessionContext session) {
    final RaftSessionContext singletonSession = sessionManager.removeSession(session.sessionId());
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
  void closeSession(RaftSessionContext session) {
    final RaftSessionContext singletonSession = sessionManager.removeSession(session.sessionId());
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
  Collection<RaftSessionContext> getSessions() {
    return sessionManager.getSessions(serviceId);
  }

  @Override
  public RaftSession getSession(long sessionId) {
    RaftSession session = sessionManager.getSession(sessionId);
    return session != null && session.getState().active() ? session : null;
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

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<RaftSession> iterator() {
    return (Iterator) getSessions().iterator();
  }
}