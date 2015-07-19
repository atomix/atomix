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

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Listeners;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.transport.Connection;

import java.util.UUID;

/**
 * Server session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class ServerSession extends Session {
  protected final Listeners<Object> listeners = new Listeners<>();

  ServerSession(long id, int client, UUID connection) {
    super(id, client, connection);
  }

  /**
   * Expires the session.
   */
  protected void expire() {
    super.expire();
  }

  /**
   * Sets the session connection.
   */
  abstract ServerSession setConnection(Connection connection);

  @Override
  @SuppressWarnings("unchecked")
  public ListenerContext<?> onReceive(Listener listener) {
    return listeners.add(listener);
  }

}
