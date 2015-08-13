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

package net.kuujo.copycat.raft.server;

import java.time.Clock;

/**
 * State machine context.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface StateMachineContext {

  /**
   * Returns the current state version.
   *
   * @return The current state version.
   */
  long version();

  /**
   * Returns the deterministic state machine time.
   *
   * @return The deterministic state machine time.
   */
  Clock time();

  /**
   * Returns the state machine sessions.
   *
   * @return The state machine sessions.
   */
  Sessions sessions();

}
