/*
 * Copyright 2015-present Open Networking Foundation
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

package io.atomix.primitives.map;

import io.atomix.cluster.NodeId;
import io.atomix.serializer.Namespace;
import io.atomix.time.Timestamp;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Builder for eventually consistent maps.
 *
 * @param <K> type for map keys
 * @param <V> type for map values
 */
public interface EventuallyConsistentMapBuilder<K, V> {

  /**
   * Sets the name of the map.
   * <p>
   * Each map is identified by a string map name. EventuallyConsistentMapImpl
   * objects in different JVMs that use the same map name will form a
   * distributed map across JVMs (provided the cluster service is aware of
   * both nodes).
   * </p>
   * <p>
   * Note: This is a mandatory parameter.
   * </p>
   *
   * @param name name of the map
   * @return this EventuallyConsistentMapBuilder
   */
  EventuallyConsistentMapBuilder<K, V> withName(String name);

  /**
   * Sets a serializer that can be used to create a serializer that
   * can serialize both the keys and values put into the map. The serializer
   * builder should be pre-populated with any classes that will be put into
   * the map.
   * <p>
   * Note: This is a mandatory parameter.
   * </p>
   *
   * @param serializer serializer
   * @return this EventuallyConsistentMapBuilder
   */
  EventuallyConsistentMapBuilder<K, V> withSerializer(Namespace serializer);

  /**
   * Sets the function to use for generating timestamps for map updates.
   * <p>
   * The client must provide an {@code BiFunction<K, V, Timestamp>}
   * which can generate timestamps for a given key. The function is free
   * to generate timestamps however it wishes, however these timestamps will
   * be used to serialize updates to the map so they must be strict enough
   * to ensure updates are properly ordered for the use case (i.e. in some
   * cases wallclock time will suffice, whereas in other cases logical time
   * will be necessary).
   * </p>
   * <p>
   * Note: This is a mandatory parameter.
   * </p>
   *
   * @param timestampProvider provides a new timestamp
   * @return this EventuallyConsistentMapBuilder
   */
  EventuallyConsistentMapBuilder<K, V> withTimestampProvider(
      BiFunction<K, V, Timestamp> timestampProvider);

  /**
   * Sets the executor to use for processing events coming in from peers.
   *
   * @param executor event executor
   * @return this EventuallyConsistentMapBuilder
   */
  EventuallyConsistentMapBuilder<K, V> withEventExecutor(
      ExecutorService executor);

  /**
   * Sets the executor to use for sending events to peers.
   *
   * @param executor event executor
   * @return this EventuallyConsistentMapBuilder
   */
  EventuallyConsistentMapBuilder<K, V> withCommunicationExecutor(
      ExecutorService executor);

  /**
   * Sets the executor to use for background anti-entropy tasks.
   *
   * @param executor event executor
   * @return this EventuallyConsistentMapBuilder
   */
  EventuallyConsistentMapBuilder<K, V> withBackgroundExecutor(
      ScheduledExecutorService executor);

  /**
   * Sets a function that can determine which peers to replicate updates to.
   * <p>
   * The default function replicates to all nodes.
   * </p>
   *
   * @param peerUpdateFunction function that takes a K, V input and returns
   *                           a collection of NodeIds to replicate the event
   *                           to
   * @return this EventuallyConsistentMapBuilder
   */
  EventuallyConsistentMapBuilder<K, V> withPeerUpdateFunction(
      BiFunction<K, V, Collection<NodeId>> peerUpdateFunction);

  /**
   * Prevents this map from writing tombstones of items that have been
   * removed. This may result in zombie items reappearing after they have
   * been removed.
   * <p>
   * The default behavior is tombstones are enabled.
   * </p>
   *
   * @return this EventuallyConsistentMapBuilder
   */
  EventuallyConsistentMapBuilder<K, V> withTombstonesDisabled();

  /**
   * Configures how often to run the anti-entropy background task.
   * <p>
   * The default anti-entropy period is 5 seconds.
   * </p>
   *
   * @param period anti-entropy period
   * @param unit   time unit for the period
   * @return this EventuallyConsistentMapBuilder
   */
  EventuallyConsistentMapBuilder<K, V> withAntiEntropyPeriod(
      long period, TimeUnit unit);

  /**
   * Configure anti-entropy to converge faster at the cost of doing more work
   * for each anti-entropy cycle. Suited to maps with low update rate where
   * convergence time is more important than throughput.
   * <p>
   * The default behavior is to do less anti-entropy work at the cost of
   * slower convergence.
   * </p>
   *
   * @return this EventuallyConsistentMapBuilder
   */
  EventuallyConsistentMapBuilder<K, V> withFasterConvergence();

  /**
   * Configure the map to persist data to disk.
   * <p>
   * The default behavior is no persistence
   * </p>
   *
   * @return this EventuallyConsistentMapBuilder
   */
  EventuallyConsistentMapBuilder<K, V> withPersistence();

  /**
   * Builds an eventually consistent map based on the configuration options
   * supplied to this builder.
   *
   * @return new eventually consistent map
   * @throws RuntimeException if a mandatory parameter is missing
   */
  EventuallyConsistentMap<K, V> build();
}
