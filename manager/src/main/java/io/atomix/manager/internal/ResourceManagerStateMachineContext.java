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

import io.atomix.copycat.server.StateMachineContext;
import io.atomix.copycat.server.session.Sessions;

import java.time.Clock;

/**
 * Resource state machine context.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ResourceManagerStateMachineContext implements StateMachineContext, AutoCloseable {
  private final StateMachineContext parent;
  final ResourceManagerSessions sessions = new ResourceManagerSessions();

  ResourceManagerStateMachineContext(StateMachineContext parent) {
    this.parent = parent;
  }

  @Override
  public long index() {
    return parent.index();
  }

  @Override
  public Clock clock() {
    return parent.clock();
  }

  @Override
  public Sessions sessions() {
    return sessions;
  }

  @Override
  public void close() {
    sessions.close();
  }

}
