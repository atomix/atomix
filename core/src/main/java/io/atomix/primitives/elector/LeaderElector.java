/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitives.elector;

import io.atomix.leadership.Leadership;
import io.atomix.leadership.LeadershipEvent;
import io.atomix.cluster.NodeId;
import io.atomix.primitives.DistributedPrimitive;

import java.util.Map;
import java.util.function.Consumer;

/**
 * {@code LeaderElector} provides the same functionality as {@link AsyncLeaderElector} with
 * the only difference that all its methods block until the corresponding operation completes.
 */
public interface LeaderElector extends DistributedPrimitive {

  @Override
  default DistributedPrimitive.Type primitiveType() {
    return DistributedPrimitive.Type.LEADER_ELECTOR;
  }

  /**
   * Attempts to become leader for a topic.
   *
   * @param topic  leadership topic
   * @param nodeId instance identifier of the node
   * @return current Leadership state of the topic
   */
  Leadership run(String topic, NodeId nodeId);

  /**
   * Withdraws from leadership race for a topic.
   *
   * @param topic leadership topic
   */
  void withdraw(String topic);

  /**
   * Attempts to promote a node to leadership displacing the current leader.
   *
   * @param topic  leadership topic
   * @param nodeId instance identifier of the new leader
   * @return {@code true} if leadership transfer was successfully executed; {@code false} if it failed.
   * This operation can return {@code false} if the node to be made new leader is not registered to
   * run for election for the topic.
   */
  boolean anoint(String topic, NodeId nodeId);

  /**
   * Attempts to promote a node to top of candidate list.
   *
   * @param topic  leadership topic
   * @param nodeId instance identifier of the new top candidate
   * @return {@code true} if node is now the top candidate. This operation can fail (i.e. return
   * {@code false}) if the node is not registered to run for election for the topic.
   */
  boolean promote(String topic, NodeId nodeId);

  /**
   * Attempts to evict a node from all leadership elections it is registered for.
   * <p>
   * If the node the current leader for a topic, this call will force the next candidate (if one exists)
   * to be promoted to leadership.
   *
   * @param nodeId node instance identifier
   */
  void evict(NodeId nodeId);

  /**
   * Returns the {@link Leadership} for the specified topic.
   *
   * @param topic leadership topic
   * @return current Leadership state of the topic
   */
  Leadership getLeadership(String topic);

  /**
   * Returns the current {@link Leadership}s for all topics.
   *
   * @return topic name to Leadership mapping
   */
  Map<String, Leadership> getLeaderships();

  /**
   * Registers a listener to be notified of Leadership changes for all topics.
   *
   * @param consumer listener to add
   */
  void addChangeListener(Consumer<LeadershipEvent> consumer);

  /**
   * Unregisters a previously registered change notification listener.
   * <p>
   * If the specified listener was not previously registered, this operation will be a noop.
   *
   * @param consumer listener to remove
   */
  void removeChangeListener(Consumer<LeadershipEvent> consumer);
}
