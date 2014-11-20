/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat;

/**
 * State machine identifier interface.<p>
 *
 * Users must implement this interface in order to provide a state machine implementation to a
 * Copycat cluster. Because Copycat guarantees deterministic state, state machine implementations
 * must be deterministic, meaning given the same set of commands in the same order, the state
 * machine should always arrive at the same state with the same output.<p>
 *
 * Copycat calls methods on the state machine implementation based on submissions to the Copycat
 * cluster. Copycat supports two different types of operations - commands and queries - and while
 * Copycat handles their behavior internally, there are some requirements of implementing each
 * operation type.<p>
 *
 * Commands are methods that alter the state machine's state. Commands are annotated with the
 * {@link net.kuujo.copycat.Command @Command} annotation. Command submissions will  always be forwarded
 * through the cluster leader and replicated before being applied to the state machine.<p>
 *
 * Queries are methods that do not alter the state machine's tate. Queries are annotated with
 * the {@link net.kuujo.copycat.Query @Query} annotation. Query submissions do not have to be logged
 * or replicated and can optionally be performed on follower nodes.<p>
 *
 * All state machine implementations support snapshotting. Snapshot support methods are default
 * methods of the {@code StateMachine} interface. To implement snapshot support, simply override
 * the {@link StateMachine#takeSnapshot()} and {@link StateMachine#installSnapshot(byte[])} methods.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateMachine {

  /**
   * Takes a snapshot of the state machine state.<p>
   *
   * This method should be overridden by state machine implementations to provide a snapshot of the
   * state machine state. When the replicated log grows too large, Copycat will take a snapshot of
   * the state machine, persist the snapshot, and wipe old entries from the log.
   *
   * @return The state machine state.
   */
  default byte[] takeSnapshot() {
    return null;
  }

  /**
   * Installs a snapshot of the state machine state.<p>
   *
   * This method should be overridden by state machine implementations to provide snapshot support.
   * When a Copycat replica recovers from failure, it may use the snapshot to recover lost state
   * prior to replaying its log. Similarly, the snapshot may be used to replicate state to followers
   * that have fallen too far out of sync.
   *
   * @param snapshot A snapshot of the state machine state.
   */
  default void installSnapshot(byte[] snapshot) {
  }

}
