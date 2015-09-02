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

import net.kuujo.copycat.raft.StateMachineContext;

import java.time.Clock;
import java.time.Instant;

/**
 * Server state machine context.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ServerStateMachineContext implements StateMachineContext {
  private long version;
  private final ServerClock clock = new ServerClock();
  private final ServerSessionManager sessions = new ServerSessionManager();

  /**
   * Updates the state machine context.
   */
  void update(long index, Instant instant) {
    version = index;
    clock.set(instant);
  }

  @Override
  public long version() {
    return version;
  }

  @Override
  public Clock clock() {
    return clock;
  }

  @Override
  public Instant now() {
    return clock.instant();
  }

  @Override
  public ServerSessionManager sessions() {
    return sessions;
  }

  @Override
  public String toString() {
    return String.format("%s[version=%d, time=%s]", getClass().getSimpleName(), version, clock);
  }

}
