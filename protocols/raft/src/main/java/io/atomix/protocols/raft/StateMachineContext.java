/*
 * Copyright 2015-present Open Networking Laboratory
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

package io.atomix.protocols.raft;

import io.atomix.protocols.raft.session.RaftSessions;
import io.atomix.time.LogicalClock;
import io.atomix.time.WallClock;

/**
 * State machine context.
 * <p>
 * The context is reflective of the current position and state of the Raft state machine. In particular,
 * it exposes the current approximate {@link StateMachineContext#getWallClock() time} and all open
 * {@link RaftSessions}.
 */
public interface StateMachineContext {

  /**
   * Returns the state machine identifier.
   *
   * @return The unique state machine identifier.
   */
  long getStateMachineId();

  /**
   * Returns the state machine name.
   *
   * @return The state machine name.
   */
  String getName();

  /**
   * Returns the state machine type.
   *
   * @return The state machine type.
   */
  String getTypeName();

  /**
   * Returns the current state machine index.
   * <p>
   * The state index is indicative of the index of the current {@link RaftCommand}
   * being applied to the server state machine. If a {@link RaftQuery} is being applied,
   * the index of the last command applied will be used.
   *
   * @return The current state machine index.
   */
  long getCurrentIndex();

  /**
   * Returns the state machine's logical clock.
   *
   * @return The state machine's logical clock.
   */
  LogicalClock getLogicalClock();

  /**
   * Returns the state machine's wall clock.
   *
   * @return The state machine's wall clock.
   */
  WallClock getWallClock();

  /**
   * Returns the state machine sessions.
   *
   * @return The state machine sessions.
   */
  RaftSessions getSessions();

}
