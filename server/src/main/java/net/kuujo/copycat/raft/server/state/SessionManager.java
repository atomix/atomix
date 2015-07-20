/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft.server.state;

import net.kuujo.copycat.transport.Connection;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Session manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class SessionManager {
  private final int memberId;
  private final RaftServerState context;
  private final Map<UUID, Connection> connections = new HashMap<>();
  private final Map<Long, ServerSession> sessions = new HashMap<>();

  SessionManager(int memberId, RaftServerState context) {
    this.memberId = memberId;
    this.context = context;
  }

  /**
   * Registers a connection.
   */
  SessionManager registerConnection(Connection connection) {
    for (ServerSession session : sessions.values()) {
      if (session.connection() == connection.id()) {
        session.setConnection(connection);
      }
    }
    connections.put(connection.id(), connection);
    return this;
  }

  /**
   * Unregisters a connection.
   */
  SessionManager unregisterConnection(Connection connection) {
    for (ServerSession session : sessions.values()) {
      if (session.connection() == connection.id()) {
        session.setConnection(null);
      }
    }
    connections.remove(connection.id());
    return this;
  }

  /**
   * Registers a session.
   */
  ServerSession registerSession(long sessionId, int memberId, UUID connectionId) {
    ServerSession session;
    if (memberId == this.memberId) {
      session = new LocalServerSession(sessionId, memberId, connectionId, context);
    } else {
      session = new RemoteServerSession(sessionId, memberId, connectionId).setConnection(connections.get(connectionId));
    }
    sessions.put(sessionId, session);
    return session;
  }

  /**
   * Unregisters a session.
   */
  SessionManager unregisterSession(ServerSession session) {
    return unregisterSession(session.id());
  }

  /**
   * Unregisters a session.
   */
  SessionManager unregisterSession(long sessionId) {
    sessions.remove(sessionId);
    return this;
  }

  /**
   * Gets a session by session ID.
   *
   * @param sessionId The session ID.
   * @return The session or {@code null} if the session doesn't exist.
   */
  ServerSession getSession(long sessionId) {
    return sessions.get(sessionId);
  }

}
