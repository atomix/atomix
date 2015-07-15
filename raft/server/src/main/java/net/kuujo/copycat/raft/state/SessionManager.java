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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.raft.protocol.Connection;

import java.util.HashMap;
import java.util.Map;

/**
 * Session manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class SessionManager {
  private final Map<Integer, Connection> connections = new HashMap<>();
  private final Map<Long, ServerSession> sessions = new HashMap<>();

  /**
   * Registers a connection.
   */
  SessionManager registerConnection(Connection connection) {
    for (ServerSession session : sessions.values()) {
      if (session.client() == connection.id()) {
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
      if (session.client() == connection.id()) {
        session.setConnection(null);
      }
    }
    connections.remove(connection.id());
    return this;
  }

  /**
   * Registers a server session.
   */
  SessionManager registerSession(ServerSession session) {
    sessions.put(session.id(), session.setConnection(connections.get(session.client())));
    return this;
  }

  /**
   * Unregisters a server session.
   */
  SessionManager unregisterSession(ServerSession session) {
    sessions.remove(session.id());
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
