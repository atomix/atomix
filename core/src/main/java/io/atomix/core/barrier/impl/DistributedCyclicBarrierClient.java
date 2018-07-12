/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.barrier.impl;

import io.atomix.primitive.event.Event;

/**
 * Distributed cyclic barrier client.
 */
public interface DistributedCyclicBarrierClient {

  /**
   * Notifies the client that the barrier is broken.
   *
   * @param id the barrier instance identifier
   */
  @Event
  void broken(long id);

  /**
   * Releases the client from waiting.
   *
   * @param id the barrier instance identifier
   * @param index the index at which the client entered the barrier
   */
  @Event
  void release(long id, int index);

  /**
   * Notifies this client to run the barrier action.
   */
  @Event
  void runAction();

}
