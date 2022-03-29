// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.service.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionListener;
import io.atomix.primitive.session.Sessions;
import io.atomix.protocols.backup.impl.PrimaryBackupSession;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * State machine sessions.
 */
public class PrimaryBackupServiceSessions implements Sessions {
  private final Map<Long, PrimaryBackupSession> sessions = Maps.newConcurrentMap();
  private final Set<SessionListener> listeners = Sets.newIdentityHashSet();

  /**
   * Adds a session to the sessions list.
   *
   * @param session The session to add.
   */
  public void openSession(PrimaryBackupSession session) {
    sessions.put(session.sessionId().id(), session);
    listeners.forEach(l -> l.onOpen(session));
  }

  /**
   * Expires and removes a session from the sessions list.
   *
   * @param session The session to remove.
   */
  public void expireSession(PrimaryBackupSession session) {
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
  public void closeSession(PrimaryBackupSession session) {
    if (sessions.remove(session.sessionId().id()) != null) {
      session.close();
      listeners.forEach(l -> l.onClose(session));
    }
  }

  @Override
  public PrimaryBackupSession getSession(long sessionId) {
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
  @SuppressWarnings("unchecked")
  public Iterator<Session> iterator() {
    return (Iterator) sessions.values().iterator();
  }
}
