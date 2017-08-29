/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.lock;

import io.atomix.cluster.NodeId;
import io.atomix.event.ListenerService;

/**
 * Lock service.
 */
public interface LockService extends ListenerService<LockEvent, LockEventListener> {

  /**
   * Returns the {@link NodeId node identifier} that is the current lock holder for a topic.
   *
   * @param topic leadership topic
   * @return node identifier of the current leader; {@code null} if there is no lock holder for the topic
   */
  NodeId getLock(String topic);

  /**
   * Attempts to acquire a lock for the given topic.
   *
   * @param topic the topic for which to acquire the lock
   */
  void lock(String topic);

  /**
   * Releases the lock if owned by the local node.
   *
   * @param topic the topic for which to release the lock
   */
  void unlock(String topic);

  /**
   * Releases a lock for the given topic, regardless of which node owns the lock.
   *
   * @param topic the topic for which to release the lock
   */
  void release(String topic);

}
