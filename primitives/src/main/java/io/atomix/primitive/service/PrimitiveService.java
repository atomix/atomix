/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.primitive.service;

import io.atomix.primitive.session.SessionListener;
import io.atomix.storage.buffer.BufferInput;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.utils.time.WallClockTimestamp;

/**
 * Base class for user-provided services.
 *
 * @see Commit
 * @see ServiceContext
 * @see ServiceExecutor
 */
public interface PrimitiveService extends SessionListener {

  /**
   * Initializes the state machine.
   *
   * @param context The state machine context.
   * @throws NullPointerException if {@code context} is null
   */
  void init(ServiceContext context);

  /**
   * Increments the Raft service time to the given timestamp.
   *
   * @param timestamp the service timestamp
   */
  void tick(WallClockTimestamp timestamp);

  /**
   * Backs up the service state to the given buffer.
   *
   * @param output the buffer to which to back up the service state
   */
  void backup(BufferOutput<?> output);

  /**
   * Restores the service state from the given buffer.
   *
   * @param input the buffer from which to restore the service state
   */
  void restore(BufferInput<?> input);

  /**
   * Applies a commit to the state machine.
   *
   * @param commit the commit to apply
   * @return the commit result
   */
  byte[] apply(Commit<byte[]> commit);

  /**
   * Closes the state machine.
   */
  default void close() {
  }
}
