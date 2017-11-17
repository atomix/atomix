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
package io.atomix.protocols.backup.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionListener;
import io.atomix.primitive.session.Sessions;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * State machine sessions.
 */
class DefaultServiceSessions implements Sessions {
  private final Map<Long, Session> sessions = Maps.newConcurrentMap();
  private final Set<SessionListener> listeners = Sets.newIdentityHashSet();

  /**
   * Adds a session to the sessions list.
   *
   * @param session The session to add.
   */
  void openSession(PrimaryBackupSession session) {
    sessions.put(session.sessionId().id(), session);
    listeners.forEach(l -> l.onOpen(session));
  }

  /**
   * Expires and removes a session from the sessions list.
   *
   * @param session The session to remove.
   */
  void expireSession(PrimaryBackupSession session) {
    if (sessions.remove(session.sessionId().id()) != null) {
      session.expire();
      listeners.forEach(l -> l.onExpire(session));
    }
  }

  /**
   * Closes and removes a session from the sessions list.
   *
   * @param session The session to remove.
   */
  void closeSession(PrimaryBackupSession session) {
    if (sessions.remove(session.sessionId().id()) != null) {
      session.close();
      listeners.forEach(l -> l.onClose(session));
    }
  }

  @Override
  public Session getSession(long sessionId) {
    return sessions.get(sessionId);
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
  public Iterator<Session> iterator() {
    return sessions.values().iterator();
  }
}