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
import io.atomix.primitives.elector.impl.DefaultLeaderElector;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Distributed mutual exclusion primitive.
 * <p>
 * {@code AsyncLeaderElector} facilitates mutually exclusive access to a shared resource by various cluster members.
 * Each resource is identified by a unique topic name and members register their desire to access the resource by
 * calling the {@link AsyncLeaderElector#run run} method. Access is grated on a FIFO basis. An instance can
 * unregister itself from the leadership election by calling {@link AsyncLeaderElector#withdraw withdraw} method.
 * If an instance currently holding the resource dies then the next instance waiting to be leader (in FIFO order)
 * will be automatically granted access to the resource.
 * <p>
 * One can register listeners to be notified when a leadership change occurs. The Listeners are notified via a
 * {@link LeadershipEvent}.
 * <p>
 * Additionally, {@code AsyncLeaderElector} provides methods to query the current state of leadership for topics.
 * <p>
 * All methods of this interface return a {@link CompletableFuture future} immediately after a successful invocation.
 * The operation itself is executed asynchronous and the returned future will be
 * {@link CompletableFuture#complete completed} when the operation finishes.
 */
public interface AsyncLeaderElector extends DistributedPrimitive {

  @Override
  default DistributedPrimitive.Type primitiveType() {
    return DistributedPrimitive.Type.LEADER_ELECTOR;
  }

  /**
   * Attempts to become leader for a topic.
   *
   * @param topic  leadership topic
   * @param nodeId instance identifier of the node
   * @return CompletableFuture that is completed with the current Leadership state of the topic
   */
  CompletableFuture<Leadership> run(String topic, NodeId nodeId);

  /**
   * Withdraws from leadership race for a topic.
   *
   * @param topic leadership topic
   * @return CompletableFuture that is completed when the withdraw is done
   */
  CompletableFuture<Void> withdraw(String topic);

  /**
   * Attempts to promote a node to leadership displacing the current leader.
   *
   * @param topic  leadership topic
   * @param nodeId instance identifier of the new leader
   * @return CompletableFuture that is completed with a boolean when the operation is done. Boolean is true if
   * leadership transfer was successfully executed; false if it failed. This operation can fail (i.e. return false)
   * if the node to be made new leader is not registering to run for election for the topic.
   */
  CompletableFuture<Boolean> anoint(String topic, NodeId nodeId);

  /**
   * Attempts to evict a node from all leadership elections it is registered for.
   * <p>
   * If the node is the current leader for a topic, this call will promote the next top candidate
   * (if one exists) to leadership.
   *
   * @param nodeId node instance identifier
   * @return CompletableFuture that is completed when the operation is done.
   */
  CompletableFuture<Void> evict(NodeId nodeId);

  /**
   * Attempts to promote a node to top of candidate list without displacing the current leader.
   *
   * @param topic  leadership topic
   * @param nodeId instance identifier of the new top candidate
   * @return CompletableFuture that is completed with a boolean when the operation is done. Boolean is true if
   * node is now the top candidate. This operation can fail (i.e. return false) if the node
   * is not registered to run for election for the topic.
   */
  CompletableFuture<Boolean> promote(String topic, NodeId nodeId);

  /**
   * Returns the {@link Leadership} for the specified topic.
   *
   * @param topic leadership topic
   * @return CompletableFuture that is completed with the current Leadership state of the topic
   */
  CompletableFuture<Leadership> getLeadership(String topic);

  /**
   * Returns the current {@link Leadership}s for all topics.
   *
   * @return CompletableFuture that is completed with the topic to Leadership mapping
   */
  CompletableFuture<Map<String, Leadership>> getLeaderships();

  /**
   * Registers a listener to be notified of Leadership changes for all topics.
   *
   * @param consumer listener to notify
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> addChangeListener(Consumer<LeadershipEvent> consumer);

  /**
   * Unregisters a previously registered change notification listener.
   * <p>
   * If the specified listener was not previously registered, this operation will be a noop.
   *
   * @param consumer listener to remove
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> removeChangeListener(Consumer<LeadershipEvent> consumer);

  /**
   * Returns a new {@link LeaderElector} that is backed by this instance.
   *
   * @param timeoutMillis timeout duration for the returned LeaderElector operations
   * @return new {@code LeaderElector} instance
   */
  default LeaderElector asLeaderElector(long timeoutMillis) {
    return new DefaultLeaderElector(this, timeoutMillis);
  }

  /**
   * Returns a new {@link LeaderElector} that is backed by this instance and with a default operation timeout.
   *
   * @return new {@code LeaderElector} instance
   */
  default LeaderElector asLeaderElector() {
    return asLeaderElector(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS);
  }
}
