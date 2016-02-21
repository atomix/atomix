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
package io.atomix.resource;

import io.atomix.copycat.Command;

/**
 * Resource write consistency level.
 * <p>
 * Write consistency levels dictate how commands should behave when submitted to a resource in the Atomic cluster.
 * Consistency levels can be applied on a per-command basis or to all operations for a given resource.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum WriteConsistency {

  /**
   * Guarantees atomicity (linearizability) for write operations and events.
   * <p>
   * Atomic write consistency enforces sequential consistency for concurrent commands from a single client by sequencing
   * commands as they're applied to the Raft state machine. If a client submits writes <em>a</em>, <em>b</em>, and <em>c</em>
   * in that order, they're guaranteed to be applied to the Raft state machine and client {@link java.util.concurrent.CompletableFuture futures}
   * are guaranteed to be completed in that order. Additionally, linearizable commands are guaranteed to be applied to the
   * server state machine some time between invocation and response, and command-related session events are guaranteed to be
   * received by clients prior to completion of the command.
   */
  ATOMIC(Command.ConsistencyLevel.LINEARIZABLE),

  /**
   * Guarantees atomicity (linearizability) for write operations and sequential consistency for events
   * triggered by a command
   * <p>
   * All commands are applied to the server state machine in program order and at some point between their invocation and
   * response (linearization point). But session events related to commands can be controlled by this consistency level.
   * The sequential consistency level guarantees that all session events related to a command will be received by the client
   * in sequential order. However, it does not guarantee that the events will be received during the invocation of the command..
   */
  SEQUENTIAL_EVENT(Command.ConsistencyLevel.SEQUENTIAL);

  private final Command.ConsistencyLevel level;

  WriteConsistency(Command.ConsistencyLevel level) {
    this.level = level;
  }

  /**
   * Returns the command consistency level.
   *
   * @return The command consistency level.
   */
  public Command.ConsistencyLevel level() {
    return level;
  }

}
