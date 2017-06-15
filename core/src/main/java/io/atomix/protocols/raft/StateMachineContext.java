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

package io.atomix.protocols.raft;

import io.atomix.protocols.raft.session.RaftSessions;

import java.time.Clock;

/**
 * State machine context.
 * <p>
 * The context is reflective of the current position and state of the Raft state machine. In particular,
 * it exposes the current approximate {@link StateMachineContext#clock() time} and all open
 * {@link RaftSessions}.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface StateMachineContext {

  /**
   * Returns the state machine identifier.
   *
   * @return The unique state machine identifier.
   */
  long id();

  /**
   * Returns the state machine name.
   *
   * @return The state machine name.
   */
  String name();

  /**
   * Returns the state machine type.
   *
   * @return The state machine type.
   */
  String type();

  /**
   * Returns the current state machine index.
   * <p>
   * The state index is indicative of the index of the current {@link Command}
   * being applied to the server state machine. If a {@link Query} is being applied,
   * the index of the last command applied will be used.
   *
   * @return The current state machine index.
   */
  long index();

  /**
   * Returns the state machine clock.
   *
   * @return The state machine clock.
   */
  Clock clock();

  /**
   * Returns the state machine sessions.
   *
   * @return The state machine sessions.
   */
  RaftSessions sessions();

}
