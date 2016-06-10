/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.manager.internal;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.session.Sessions;

import java.util.*;

/**
 * Resource manager sessions.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ResourceManagerSessions implements Sessions, AutoCloseable {
  private ManagedResourceSession first;
  private final Map<Long, ManagedResourceSession> sessions = new HashMap<>();
  private final Set<SessionListener> listeners = new HashSet<>();

  @Override
  public ManagedResourceSession session(long sessionId) {
    return sessions.get(sessionId);
  }

  void register(ManagedResourceSession session) {
    // If this is the first registered session, store it so it can be removed once all sessions are removed.
    if (first == null)
      first = session;

    // If a session was already registered for the session ID, release the new commit.
    if (sessions.containsKey(session.id())) {
      session.commit.close();
    } else {
      sessions.put(session.id(), session);
      for (SessionListener listener : listeners) {
        listener.register(session);
      }
    }
  }

  void unregister(long sessionId) {
    ManagedResourceSession session = sessions.get(sessionId);
    if (session != null) {
      for (SessionListener listener : listeners) {
        listener.unregister(session);
      }
    }
  }

  void expire(long sessionId) {
    ManagedResourceSession session = sessions.get(sessionId);
    if (session != null) {
      for (SessionListener listener : listeners) {
        listener.expire(session);
      }
    }
  }

  void close(long sessionId) {
    ManagedResourceSession session = sessions.remove(sessionId);
    if (session != null) {
      for (SessionListener listener : listeners) {
        listener.close(session);
      }
      if (session.commit != first.commit) {
        session.commit.close();
      }
    }
  }

  @Override
  public Sessions addListener(SessionListener listener) {
    listeners.add(Assert.notNull(listener, "listener"));
    return this;
  }

  @Override
  public Sessions removeListener(SessionListener listener) {
    listeners.remove(Assert.notNull(listener, "listener"));
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<ServerSession> iterator() {
    return (Iterator) sessions.values().iterator();
  }

  @Override
  public void close() {
    for (ManagedResourceSession session : sessions.values()) {
      session.commit.close();
    }
  }

}
