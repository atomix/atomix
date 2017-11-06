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
package io.atomix.primitives.leadership;

import io.atomix.primitives.AsyncPrimitive;
import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.leadership.impl.BlockingLeaderElector;

import java.util.concurrent.CompletableFuture;

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
public interface AsyncLeaderElector<T> extends AsyncPrimitive {

  @Override
  default DistributedPrimitive.Type primitiveType() {
    return DistributedPrimitive.Type.LEADER_ELECTOR;
  }

  /**
   * Attempts to become leader for a topic.
   *
   * @param identifier instance identifier of the node
   * @return CompletableFuture that is completed with the current Leadership state of the topic
   */
  CompletableFuture<Leadership<T>> run(T identifier);

  /**
   * Withdraws from leadership race for a topic.
   *
   * @return CompletableFuture that is completed when the withdraw is done
   */
  CompletableFuture<Void> withdraw();

  /**
   * Attempts to promote a node to leadership displacing the current leader.
   *
   * @param identifier instance identifier of the new leader
   * @return CompletableFuture that is completed with a boolean when the operation is done. Boolean is true if
   * leadership transfer was successfully executed; false if it failed. This operation can fail (i.e. return false)
   * if the node to be made new leader is not registering to run for election for the topic.
   */
  CompletableFuture<Boolean> anoint(T identifier);

  /**
   * Attempts to evict a node from all leadership elections it is registered for.
   * <p>
   * If the node is the current leader for a topic, this call will promote the next top candidate
   * (if one exists) to leadership.
   *
   * @param identifier node instance identifier
   * @return CompletableFuture that is completed when the operation is done.
   */
  CompletableFuture<Void> evict(T identifier);

  /**
   * Attempts to promote a node to top of candidate list without displacing the current leader.
   *
   * @param identifier instance identifier of the new top candidate
   * @return CompletableFuture that is completed with a boolean when the operation is done. Boolean is true if
   * node is now the top candidate. This operation can fail (i.e. return false) if the node
   * is not registered to run for election for the topic.
   */
  CompletableFuture<Boolean> promote(T identifier);

  /**
   * Returns the {@link Leadership} for the specified topic.
   *
   * @return CompletableFuture that is completed with the current Leadership state of the topic
   */
  CompletableFuture<Leadership<T>> getLeadership();

  /**
   * Registers a listener to be notified of Leadership changes for all topics.
   *
   * @param listener listener to notify
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> addListener(LeadershipEventListener<T> listener);

  /**
   * Unregisters a previously registered change notification listener.
   * <p>
   * If the specified listener was not previously registered, this operation will be a noop.
   *
   * @param listener listener to remove
   * @return CompletableFuture that is completed when the operation completes
   */
  CompletableFuture<Void> removeListener(LeadershipEventListener<T> listener);

  /**
   * Returns a new {@link LeaderElector} that is backed by this instance and with a default operation timeout.
   *
   * @return new {@code LeaderElector} instance
   */
  default LeaderElector<T> asLeaderElector() {
    return asLeaderElector(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS);
  }

  /**
   * Returns a new {@link LeaderElector} that is backed by this instance.
   *
   * @param timeoutMillis timeout duration for the returned LeaderElector operations
   * @return new {@code LeaderElector} instance
   */
  default LeaderElector<T> asLeaderElector(long timeoutMillis) {
    return new BlockingLeaderElector<>(this, timeoutMillis);
  }
}
