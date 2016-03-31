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
package io.atomix;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Managed;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.collections.DistributedMap;
import io.atomix.collections.DistributedMultiMap;
import io.atomix.collections.DistributedQueue;
import io.atomix.collections.DistributedSet;
import io.atomix.concurrent.DistributedLock;
import io.atomix.group.DistributedGroup;
import io.atomix.manager.ResourceClient;
import io.atomix.manager.ResourceManager;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceType;
import io.atomix.variables.DistributedLong;
import io.atomix.variables.DistributedValue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Base type for creating and managing distributed {@link Resource resources} in a Atomix cluster.
 * <p>
 * Resources are user provided stateful objects backed by a distributed state machine. This class facilitates the
 * creation and management of {@link Resource} objects via a filesystem like interface. There is a
 * one-to-one relationship between keys and resources, so each key can be associated with one and only one resource.
 * <p>
 * To create a resource, pass the resource {@link java.lang.Class} to the {@link Atomix#getResource(String, ResourceType)} method.
 * When a resource is created, the {@link io.atomix.copycat.server.StateMachine} associated with the resource will be created on each Raft server
 * and future operations submitted for that resource will be applied to the state machine. Internally, resource state
 * machines are multiplexed across a shared Raft log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Atomix implements ResourceManager<Atomix>, Managed<Atomix> {
  static final Collection<ResourceType> RESOURCES = Arrays.asList(
    new ResourceType(DistributedMap.class),
    new ResourceType(DistributedMultiMap.class),
    new ResourceType(DistributedSet.class),
    new ResourceType(DistributedQueue.class),
    new ResourceType(DistributedValue.class),
    new ResourceType(DistributedLong.class),
    new ResourceType(DistributedLock.class),
    new ResourceType(DistributedGroup.class)
  );

  final ResourceClient client;

  protected Atomix(ResourceClient client) {
    this.client = Assert.notNull(client, "client");
  }

  @Override
  public ThreadContext context() {
    return client.context();
  }

  /**
   * Returns the Atomix serializer.
   * <p>
   * Serializable types registered on the returned serializer are reflected throughout the system. Types
   * registered on a client must also be registered on all replicas to be transported across the wire.
   *
   * @return The Atomix serializer.
   */
  public Serializer serializer() {
    return client.client().serializer();
  }

  /**
   * Gets or creates a distributed map with default configuration and options.
   * <p>
   * The returned {@link DistributedMap} replicates and stores map entries in memory in a {@link java.util.HashMap}.
   * The size of the map is limited by the memory available to the smallest replica in the cluster. The map
   * requires non-null keys but allows null values.
   * <p>
   * Map key and value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the map.
   * <p>
   * If no map exists at the given {@code key}, a new map will be created. If a map with the given key
   * already exists, a reference to the map will be returned in the {@link CompletableFuture}. The map
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the map is guaranteed to be
   * visible by all clients and replicas in the cluster.
   *
   * @param key The resource key.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the map has been created.
   */
  public <K, V> CompletableFuture<DistributedMap<K, V>> getMap(String key) {
    return getResource(key, DistributedMap.class);
  }

  /**
   * Gets or creates a distributed map with a cluster-wide configuration.
   * <p>
   * The returned {@link DistributedMap} replicates and stores map entries in memory in a {@link java.util.HashMap}.
   * The size of the map is limited by the memory available to the smallest replica in the cluster. The map
   * requires non-null keys but allows null values.
   * <p>
   * Map key and value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the map.
   * <p>
   * If no map exists at the given {@code key}, a new map will be created. If a map with the given key
   * already exists, a reference to the map will be returned in the {@link CompletableFuture}. The map
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the map is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided map {@link DistributedMap.Config Config} will be used to configure the cluster-wide map.
   * If another process previously configured the map with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   *
   * @param key The resource key.
   * @param config The cluster-wide map configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the map has been created.
   */
  public <K, V> CompletableFuture<DistributedMap<K, V>> getMap(String key, DistributedMap.Config config) {
    return getResource(key, DistributedMap.class, config);
  }

  /**
   * Gets or creates a distributed map with local options.
   * <p>
   * The returned {@link DistributedMap} replicates and stores map entries in memory in a {@link java.util.HashMap}.
   * The size of the map is limited by the memory available to the smallest replica in the cluster. The map
   * requires non-null keys but allows null values.
   * <p>
   * Map key and value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the map.
   * <p>
   * If no map exists at the given {@code key}, a new map will be created. If a map with the given key
   * already exists, a reference to the map will be returned in the {@link CompletableFuture}. The map
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the map is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided map {@link DistributedMap.Options options} will be used to configure only the local map
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedMap.Config Config}.
   *
   * @param key The resource key.
   * @param options The local map options.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the map has been created.
   */
  public <K, V> CompletableFuture<DistributedMap<K, V>> getMap(String key, DistributedMap.Options options) {
    return getResource(key, DistributedMap.class, options);
  }

  /**
   * Gets or creates a distributed map with a cluster-wide configuration and local options.
   * <p>
   * The returned {@link DistributedMap} replicates and stores map entries in memory in a {@link java.util.HashMap}.
   * The size of the map is limited by the memory available to the smallest replica in the cluster. The map
   * requires non-null keys but allows null values.
   * <p>
   * Map key and value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the map.
   * <p>
   * If no map exists at the given {@code key}, a new map will be created. If a map with the given key
   * already exists, a reference to the map will be returned in the {@link CompletableFuture}. The map
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the map is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided map {@link DistributedMap.Config Config} will be used to configure the cluster-wide map.
   * If another process previously configured the map with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   * <p>
   * The provided map {@link DistributedMap.Options options} will be used to configure only the local map
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedMap.Config Config}.
   *
   * @param key The resource key.
   * @param config The cluster-wide map configuration.
   * @param options The local map options.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the map has been created.
   */
  public <K, V> CompletableFuture<DistributedMap<K, V>> getMap(String key, DistributedMap.Config config, DistributedMap.Options options) {
    return getResource(key, DistributedMap.class, config, options);
  }

  /**
   * Gets or creates a distributed multi map with default configuration and options.
   * <p>
   * The multi-map is a map of keys to a collection of values. The order of value collections is dependent
   * upon the map {@link DistributedMultiMap.Config configuration}. The map requires non-null keys but allows
   * null values.
   * <p>
   * Map key and value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the map.
   * <p>
   * If no map exists at the given {@code key}, a new map will be created. If a map with the given key
   * already exists, a reference to the map will be returned in the {@link CompletableFuture}. The map
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the map is guaranteed to be
   * visible by all clients and replicas in the cluster.
   *
   * @param key The resource key.
   * @param <K> The multi map key type.
   * @param <V> The multi map value type.
   * @return A completable future to be completed once the multi map has been created.
   */
  public <K, V> CompletableFuture<DistributedMultiMap<K, V>> getMultiMap(String key) {
    return getResource(key, DistributedMultiMap.class);
  }

  /**
   * Gets or creates a distributed multi map with a cluster-wide configuration.
   * <p>
   * The multi-map is a map of keys to a collection of values. The order of value collections is dependent
   * upon the map {@link DistributedMultiMap.Config configuration}. The map requires non-null keys but allows
   * null values.
   * <p>
   * Map key and value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the map.
   * <p>
   * If no map exists at the given {@code key}, a new map will be created. If a map with the given key
   * already exists, a reference to the map will be returned in the {@link CompletableFuture}. The map
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the map is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided map {@link DistributedMultiMap.Config Config} will be used to configure the cluster-wide map.
   * If another process previously configured the map with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   *
   * @param key The resource key.
   * @param config The cluster-wide multi map configuration.
   * @param <K> The multi map key type.
   * @param <V> The multi map value type.
   * @return A completable future to be completed once the multi map has been created.
   */
  public <K, V> CompletableFuture<DistributedMultiMap<K, V>> getMultiMap(String key, DistributedMultiMap.Config config) {
    return getResource(key, DistributedMultiMap.class, config);
  }

  /**
   * Gets or creates a distributed multi map with local options.
   * <p>
   * The multi-map is a map of keys to a collection of values. The order of value collections is dependent
   * upon the map {@link DistributedMultiMap.Config configuration}. The map requires non-null keys but allows
   * null values.
   * <p>
   * Map key and value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the map.
   * <p>
   * If no map exists at the given {@code key}, a new map will be created. If a map with the given key
   * already exists, a reference to the map will be returned in the {@link CompletableFuture}. The map
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the map is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided map {@link DistributedMultiMap.Options options} will be used to configure only the local map
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedMultiMap.Config Config}.
   *
   * @param key The resource key.
   * @param options The local multi map options.
   * @param <K> The multi map key type.
   * @param <V> The multi map value type.
   * @return A completable future to be completed once the multi map has been created.
   */
  public <K, V> CompletableFuture<DistributedMultiMap<K, V>> getMultiMap(String key, DistributedMultiMap.Options options) {
    return getResource(key, DistributedMultiMap.class, options);
  }

  /**
   * Gets or creates a distributed multi map with a cluster-wide configuration and local options.
   * <p>
   * The multi-map is a map of keys to a collection of values. The order of value collections is dependent
   * upon the map {@link DistributedMultiMap.Config configuration}. The map requires non-null keys but allows
   * null values.
   * <p>
   * Map key and value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the map.
   * <p>
   * If no map exists at the given {@code key}, a new map will be created. If a map with the given key
   * already exists, a reference to the map will be returned in the {@link CompletableFuture}. The map
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the map is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided map {@link DistributedMultiMap.Config Config} will be used to configure the cluster-wide map.
   * If another process previously configured the map with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   * <p>
   * The provided map {@link DistributedMultiMap.Options options} will be used to configure only the local map
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedMultiMap.Config Config}.
   *
   * @param key The resource key.
   * @param config The cluster-wide multi map configuration.
   * @param options The local multi map options.
   * @param <K> The multi map key type.
   * @param <V> The multi map value type.
   * @return A completable future to be completed once the multi map has been created.
   */
  public <K, V> CompletableFuture<DistributedMultiMap<K, V>> getMultiMap(String key, DistributedMultiMap.Config config, DistributedMultiMap.Options options) {
    return getResource(key, DistributedMultiMap.class, config, options);
  }

  /**
   * Gets or creates a distributed set with default configuration and options.
   * <p>
   * The returned set replicates a unique set of values. Values must be non-null and must be serializable
   * with the local {@code Atomix} instance {@link Serializer} and all {@link AtomixReplica} instances.
   * By default, all primitives and most collections are serializable. For custom classes, users must
   * {@link Serializer#register(Class)} serializable types <em>before</em> constructing the set.
   * <p>
   * If no set exists at the given {@code key}, a new set will be created. If a set with the given key
   * already exists, a reference to the set will be returned in the {@link CompletableFuture}. The set
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the set is guaranteed to be
   * visible by all clients and replicas in the cluster.
   *
   * @param key The resource key.
   * @param <T> The value type.
   * @return A completable future to be completed once the set has been created.
   */
  public <T> CompletableFuture<DistributedSet<T>> getSet(String key) {
    return getResource(key, DistributedSet.class);
  }

  /**
   * Gets or creates a distributed set with a cluster-wide configuration.
   * <p>
   * The returned set replicates a unique set of values. Values must be non-null and must be serializable
   * with the local {@code Atomix} instance {@link Serializer} and all {@link AtomixReplica} instances.
   * By default, all primitives and most collections are serializable. For custom classes, users must
   * {@link Serializer#register(Class)} serializable types <em>before</em> constructing the set.
   * <p>
   * If no set exists at the given {@code key}, a new set will be created. If a set with the given key
   * already exists, a reference to the set will be returned in the {@link CompletableFuture}. The set
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the set is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided set {@link DistributedSet.Config Config} will be used to configure the cluster-wide set.
   * If another process previously configured the set with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   *
   * @param key The resource key.
   * @param config The cluster-wide set configuration.
   * @param <T> The value type.
   * @return A completable future to be completed once the set has been created.
   */
  public <T> CompletableFuture<DistributedSet<T>> getSet(String key, DistributedSet.Config config) {
    return getResource(key, DistributedSet.class, config);
  }

  /**
   * Gets or creates a distributed set with local options.
   * <p>
   * The returned set replicates a unique set of values. Values must be non-null and must be serializable
   * with the local {@code Atomix} instance {@link Serializer} and all {@link AtomixReplica} instances.
   * By default, all primitives and most collections are serializable. For custom classes, users must
   * {@link Serializer#register(Class)} serializable types <em>before</em> constructing the set.
   * <p>
   * If no set exists at the given {@code key}, a new set will be created. If a set with the given key
   * already exists, a reference to the set will be returned in the {@link CompletableFuture}. The set
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the set is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided set {@link DistributedSet.Options options} will be used to configure only the local set
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedSet.Config Config}.
   *
   * @param key The resource key.
   * @param options The local set options.
   * @param <T> The value type.
   * @return A completable future to be completed once the set has been created.
   */
  public <T> CompletableFuture<DistributedSet<T>> getSet(String key, DistributedSet.Options options) {
    return getResource(key, DistributedSet.class, options);
  }

  /**
   * Gets or creates a distributed set with a cluster-wide configuration and local options.
   * <p>
   * The returned set replicates a unique set of values. Values must be non-null and must be serializable
   * with the local {@code Atomix} instance {@link Serializer} and all {@link AtomixReplica} instances.
   * By default, all primitives and most collections are serializable. For custom classes, users must
   * {@link Serializer#register(Class)} serializable types <em>before</em> constructing the set.
   * <p>
   * If no set exists at the given {@code key}, a new set will be created. If a set with the given key
   * already exists, a reference to the set will be returned in the {@link CompletableFuture}. The set
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the set is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided set {@link DistributedSet.Config Config} will be used to configure the cluster-wide set.
   * If another process previously configured the set with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   * <p>
   * The provided set {@link DistributedSet.Options options} will be used to configure only the local set
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedSet.Config Config}.
   *
   * @param key The resource key.
   * @param config The cluster-wide set configuration.
   * @param options The local set options.
   * @param <T> The value type.
   * @return A completable future to be completed once the set has been created.
   */
  public <T> CompletableFuture<DistributedSet<T>> getSet(String key, DistributedSet.Config config, DistributedSet.Options options) {
    return getResource(key, DistributedSet.class, config, options);
  }

  /**
   * Gets or creates a distributed queue with default configuration and options.
   * <p>
   * The returned queue is backed by a replicated {@link java.util.ArrayDeque}. Queue values can be
   * {@code null}. The size of the queue is limited by the amount of memory available to the smallest
   * replica in the cluster.
   * <p>
   * Queue value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the queue.
   * <p>
   * If no queue exists at the given {@code key}, a new queue will be created. If a queue with the given key
   * already exists, a reference to the queue will be returned in the {@link CompletableFuture}. The queue
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the queue is guaranteed to be
   * visible by all clients and replicas in the cluster.
   *
   * @param key The resource key.
   * @param <T> The value type.
   * @return A completable future to be completed once the queue has been created.
   */
  public <T> CompletableFuture<DistributedQueue<T>> getQueue(String key) {
    return getResource(key, DistributedQueue.class);
  }

  /**
   * Gets or creates a distributed queue with a cluster-wide configuration.
   * <p>
   * The returned queue is backed by a replicated {@link java.util.ArrayDeque}. Queue values can be
   * {@code null}. The size of the queue is limited by the amount of memory available to the smallest
   * replica in the cluster.
   * <p>
   * Queue value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the queue.
   * <p>
   * If no queue exists at the given {@code key}, a new queue will be created. If a queue with the given key
   * already exists, a reference to the queue will be returned in the {@link CompletableFuture}. The queue
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the queue is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided queue {@link DistributedQueue.Config Config} will be used to configure the cluster-wide queue.
   * If another process previously configured the queue with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   *
   * @param key The resource key.
   * @param config The cluster-wide queue configuration.
   * @param <T> The value type.
   * @return A completable future to be completed once the queue has been created.
   */
  public <T> CompletableFuture<DistributedQueue<T>> getQueue(String key, DistributedQueue.Config config) {
    return getResource(key, DistributedQueue.class, config);
  }

  /**
   * Gets or creates a distributed queue with local options.
   * <p>
   * The returned queue is backed by a replicated {@link java.util.ArrayDeque}. Queue values can be
   * {@code null}. The size of the queue is limited by the amount of memory available to the smallest
   * replica in the cluster.
   * <p>
   * Queue value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the queue.
   * <p>
   * If no queue exists at the given {@code key}, a new queue will be created. If a queue with the given key
   * already exists, a reference to the queue will be returned in the {@link CompletableFuture}. The queue
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the queue is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided queue {@link DistributedQueue.Options options} will be used to configure only the local queue
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedQueue.Config Config}.
   *
   * @param key The resource key.
   * @param options The local queue options.
   * @param <T> The value type.
   * @return A completable future to be completed once the queue has been created.
   */
  public <T> CompletableFuture<DistributedQueue<T>> getQueue(String key, DistributedQueue.Options options) {
    return getResource(key, DistributedQueue.class, options);
  }

  /**
   * Gets or creates a distributed queue with a cluster-wide configuration and local options.
   * <p>
   * The returned queue is backed by a replicated {@link java.util.ArrayDeque}. Queue values can be
   * {@code null}. The size of the queue is limited by the amount of memory available to the smallest
   * replica in the cluster.
   * <p>
   * Queue value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the queue.
   * <p>
   * If no queue exists at the given {@code key}, a new queue will be created. If a queue with the given key
   * already exists, a reference to the queue will be returned in the {@link CompletableFuture}. The queue
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the queue is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided queue {@link DistributedQueue.Config Config} will be used to configure the cluster-wide queue.
   * If another process previously configured the queue with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   * <p>
   * The provided queue {@link DistributedQueue.Options options} will be used to configure only the local queue
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedQueue.Config Config}.
   *
   * @param key The resource key.
   * @param config The cluster-wide queue configuration.
   * @param options The local queue options.
   * @param <T> The value type.
   * @return A completable future to be completed once the queue has been created.
   */
  public <T> CompletableFuture<DistributedQueue<T>> getQueue(String key, DistributedQueue.Config config, DistributedQueue.Options options) {
    return getResource(key, DistributedQueue.class, config, options);
  }

  /**
   * Gets or creates a distributed value with default configuration and options.
   * <p>
   * The returned resource is a replicated representation of an arbitrary value. Values may be {@code null}.
   * Value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the value.
   * <p>
   * If no value exists at the given {@code key}, a new value will be created. If a value with the given key
   * already exists, a reference to the value will be returned in the {@link CompletableFuture}. The value
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the value is guaranteed to be
   * visible by all clients and replicas in the cluster.
   *
   * @param key The resource key.
   * @param <T> The value type.
   * @return A completable future to be completed once the value has been created.
   */
  public <T> CompletableFuture<DistributedValue<T>> getValue(String key) {
    return getResource(key, DistributedValue.class);
  }

  /**
   * Gets or creates a distributed value with a cluster-wide configuration.
   * <p>
   * The returned resource is a replicated representation of an arbitrary value. Values may be {@code null}.
   * Value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the value.
   * <p>
   * If no value exists at the given {@code key}, a new value will be created. If a value with the given key
   * already exists, a reference to the value will be returned in the {@link CompletableFuture}. The value
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the value is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided value {@link DistributedValue.Config Config} will be used to configure the cluster-wide value.
   * If another process previously configured the value with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   *
   * @param key The resource key.
   * @param config The cluster-wide value configuration.
   * @param <T> The value type.
   * @return A completable future to be completed once the value has been created.
   */
  public <T> CompletableFuture<DistributedValue<T>> getValue(String key, DistributedValue.Config config) {
    return getResource(key, DistributedValue.class, config);
  }

  /**
   * Gets or creates a distributed value with local options.
   * <p>
   * The returned resource is a replicated representation of an arbitrary value. Values may be {@code null}.
   * Value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the value.
   * <p>
   * If no value exists at the given {@code key}, a new value will be created. If a value with the given key
   * already exists, a reference to the value will be returned in the {@link CompletableFuture}. The value
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the value is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided value {@link DistributedValue.Options options} will be used to configure only the local value
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedValue.Config Config}.
   *
   * @param key The resource key.
   * @param options The local value options.
   * @param <T> The value type.
   * @return A completable future to be completed once the value has been created.
   */
  public <T> CompletableFuture<DistributedValue<T>> getValue(String key, DistributedValue.Options options) {
    return getResource(key, DistributedValue.class, options);
  }

  /**
   * Gets or creates a distributed value with a cluster-wide configuration and local options.
   * <p>
   * The returned resource is a replicated representation of an arbitrary value. Values may be {@code null}.
   * Value types must be serializable with the local {@code Atomix} instance {@link Serializer}
   * and all {@link AtomixReplica} instances. By default, all primitives and most collections are serializable.
   * For custom classes, users must {@link Serializer#register(Class)} serializable types <em>before</em>
   * constructing the value.
   * <p>
   * If no value exists at the given {@code key}, a new value will be created. If a value with the given key
   * already exists, a reference to the value will be returned in the {@link CompletableFuture}. The value
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the value is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided value {@link DistributedValue.Config Config} will be used to configure the cluster-wide value.
   * If another process previously configured the value with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   * <p>
   * The provided value {@link DistributedValue.Options options} will be used to configure only the local value
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedValue.Config Config}.
   *
   * @param key The resource key.
   * @param config The cluster-wide value configuration.
   * @param options The local value options.
   * @param <T> The value type.
   * @return A completable future to be completed once the value has been created.
   */
  public <T> CompletableFuture<DistributedValue<T>> getValue(String key, DistributedValue.Config config, DistributedValue.Options options) {
    return getResource(key, DistributedValue.class, config, options);
  }

  /**
   * Gets or creates a distributed long with default configuration and options.
   * <p>
   * The returned resource is an asynchronous distributed object similar to {@link java.util.concurrent.atomic.AtomicLong}.
   * Operations that modify the value are guaranteed to be atomic, and changes to the state of the value
   * are guaranteed to be visible by all clients and replicas when {@link io.atomix.resource.ReadConsistency#ATOMIC}
   * or {@link io.atomix.resource.ReadConsistency#ATOMIC_LEASE} is enabled.
   * <p>
   * If no long exists at the given {@code key}, a new long will be created. If a long with the given key
   * already exists, a reference to the long will be returned in the {@link CompletableFuture}. The long
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the long is guaranteed to be
   * visible by all clients and replicas in the cluster.
   *
   * @param key The resource key.
   * @return A completable future to be completed once the long has been created.
   */
  public CompletableFuture<DistributedLong> getLong(String key) {
    return getResource(key, DistributedLong.class);
  }

  /**
   * Gets or creates a distributed long with a cluster-wide configuration.
   * <p>
   * The returned resource is an asynchronous distributed object similar to {@link java.util.concurrent.atomic.AtomicLong}.
   * Operations that modify the value are guaranteed to be atomic, and changes to the state of the value
   * are guaranteed to be visible by all clients and replicas when {@link io.atomix.resource.ReadConsistency#ATOMIC}
   * or {@link io.atomix.resource.ReadConsistency#ATOMIC_LEASE} is enabled.
   * <p>
   * If no long exists at the given {@code key}, a new long will be created. If a long with the given key
   * already exists, a reference to the long will be returned in the {@link CompletableFuture}. The long
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the long is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided long {@link DistributedLong.Config Config} will be used to configure the cluster-wide long.
   * If another process previously configured the long with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   *
   * @param key The resource key.
   * @param config The cluster-wide long configuration.
   * @return A completable future to be completed once the long has been created.
   */
  public CompletableFuture<DistributedLong> getLong(String key, DistributedLong.Config config) {
    return getResource(key, DistributedLong.class, config);
  }

  /**
   * Gets or creates a distributed long with local options.
   * <p>
   * The returned resource is an asynchronous distributed object similar to {@link java.util.concurrent.atomic.AtomicLong}.
   * Operations that modify the value are guaranteed to be atomic, and changes to the state of the value
   * are guaranteed to be visible by all clients and replicas when {@link io.atomix.resource.ReadConsistency#ATOMIC}
   * or {@link io.atomix.resource.ReadConsistency#ATOMIC_LEASE} is enabled.
   * <p>
   * If no long exists at the given {@code key}, a new long will be created. If a long with the given key
   * already exists, a reference to the long will be returned in the {@link CompletableFuture}. The long
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the long is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided long {@link DistributedLong.Options options} will be used to configure only the local long
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedLong.Config Config}.
   *
   * @param key The resource key.
   * @param options The local long options.
   * @return A completable future to be completed once the long has been created.
   */
  public CompletableFuture<DistributedLong> getLong(String key, DistributedLong.Options options) {
    return getResource(key, DistributedLong.class, options);
  }

  /**
   * Gets or creates a distributed long with a cluster-wide configuration and local options.
   * <p>
   * The returned resource is an asynchronous distributed object similar to {@link java.util.concurrent.atomic.AtomicLong}.
   * Operations that modify the value are guaranteed to be atomic, and changes to the state of the value
   * are guaranteed to be visible by all clients and replicas when {@link io.atomix.resource.ReadConsistency#ATOMIC}
   * or {@link io.atomix.resource.ReadConsistency#ATOMIC_LEASE} is enabled.
   * <p>
   * If no long exists at the given {@code key}, a new long will be created. If a long with the given key
   * already exists, a reference to the long will be returned in the {@link CompletableFuture}. The long
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the long is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided long {@link DistributedLong.Config Config} will be used to configure the cluster-wide long.
   * If another process previously configured the long with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   * <p>
   * The provided long {@link DistributedLong.Options options} will be used to configure only the local long
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedLong.Config Config}.
   *
   * @param key The resource key.
   * @param config The cluster-wide long configuration.
   * @param options The local long options.
   * @return A completable future to be completed once the long has been created.
   */
  public CompletableFuture<DistributedLong> getLong(String key, DistributedLong.Config config, DistributedLong.Options options) {
    return getResource(key, DistributedLong.class, config, options);
  }

  /**
   * Gets or creates a distributed lock with default configuration and options.
   * <p>
   * The returned resource is a cluster-wide distributed lock. The lock is fair, meaning the lock will be
   * granted to the longest waiting process. In the event that a lock holder crashes or is partitioned, the
   * lock will be automatically released once the lock holder's session expires, and the lock will be granted
   * to the next process waiting in the lock queue.
   * <p>
   * If no lock exists at the given {@code key}, a new lock will be created. If a lock with the given key
   * already exists, a reference to the lock will be returned in the {@link CompletableFuture}. The lock
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the lock is guaranteed to be
   * visible by all clients and replicas in the cluster.
   *
   * @param key The resource key.
   * @return A completable future to be completed once the lock has been created.
   */
  public CompletableFuture<DistributedLock> getLock(String key) {
    return getResource(key, DistributedLock.class);
  }

  /**
   * Gets or creates a distributed lock with a cluster-wide configuration.
   * <p>
   * The returned resource is a cluster-wide distributed lock. The lock is fair, meaning the lock will be
   * granted to the longest waiting process. In the event that a lock holder crashes or is partitioned, the
   * lock will be automatically released once the lock holder's session expires, and the lock will be granted
   * to the next process waiting in the lock queue.
   * <p>
   * If no lock exists at the given {@code key}, a new lock will be created. If a lock with the given key
   * already exists, a reference to the lock will be returned in the {@link CompletableFuture}. The lock
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the lock is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided lock {@link DistributedLock.Config Config} will be used to configure the cluster-wide lock.
   * If another process previously configured the lock with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   *
   * @param key The resource key.
   * @param config The cluster-wide lock configuration.
   * @return A completable future to be completed once the lock has been created.
   */
  public CompletableFuture<DistributedLock> getLock(String key, DistributedLock.Config config) {
    return getResource(key, DistributedLock.class, config);
  }

  /**
   * Gets or creates a distributed lock with local options.
   * <p>
   * The returned resource is a cluster-wide distributed lock. The lock is fair, meaning the lock will be
   * granted to the longest waiting process. In the event that a lock holder crashes or is partitioned, the
   * lock will be automatically released once the lock holder's session expires, and the lock will be granted
   * to the next process waiting in the lock queue.
   * <p>
   * If no lock exists at the given {@code key}, a new lock will be created. If a lock with the given key
   * already exists, a reference to the lock will be returned in the {@link CompletableFuture}. The lock
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the lock is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided lock {@link DistributedLock.Options options} will be used to configure only the local lock
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedLock.Config Config}.
   *
   * @param key The resource key.
   * @param options The local lock options.
   * @return A completable future to be completed once the lock has been created.
   */
  public CompletableFuture<DistributedLock> getLock(String key, DistributedLock.Options options) {
    return getResource(key, DistributedLock.class, options);
  }

  /**
   * Gets or creates a distributed lock with a cluster-wide configuration and local options.
   * <p>
   * The returned resource is a cluster-wide distributed lock. The lock is fair, meaning the lock will be
   * granted to the longest waiting process. In the event that a lock holder crashes or is partitioned, the
   * lock will be automatically released once the lock holder's session expires, and the lock will be granted
   * to the next process waiting in the lock queue.
   * <p>
   * If no lock exists at the given {@code key}, a new lock will be created. If a lock with the given key
   * already exists, a reference to the lock will be returned in the {@link CompletableFuture}. The lock
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the lock is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided lock {@link DistributedLock.Config Config} will be used to configure the cluster-wide lock.
   * If another process previously configured the lock with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   * <p>
   * The provided lock {@link DistributedLock.Options options} will be used to configure only the local lock
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedLock.Config Config}.
   *
   * @param key The resource key.
   * @param config The cluster-wide lock configuration.
   * @param options The local lock options.
   * @return A completable future to be completed once the lock has been created.
   */
  public CompletableFuture<DistributedLock> getLock(String key, DistributedLock.Config config, DistributedLock.Options options) {
    return getResource(key, DistributedLock.class, config, options);
  }

  /**
   * Gets or creates a distributed group for managing group membership and leader elections.
   * <p>
   * The returned resource can be used to perform a variety of common distributed systems tasks, most notable
   * of which are group membership and leader election. The {@link DistributedGroup} can be used to identify
   * members of a group or to join a group. Members of the group are guaranteed to be consistent across all
   * clients and replicas. If a member of the group is partitioned or fails, the member will be removed once
   * its session expires.
   * <p>
   * If no group exists at the given {@code key}, a new group will be created. If a group with the given key
   * already exists, a reference to the group will be returned in the {@link CompletableFuture}. The group
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the group is guaranteed to be
   * visible by all clients and replicas in the cluster.
   *
   * @param key The resource key.
   * @return A completable future to be completed once the group has been created.
   */
  public CompletableFuture<DistributedGroup> getGroup(String key) {
    return getResource(key, DistributedGroup.class);
  }

  /**
   * Gets or creates a distributed group for managing group membership and leader elections.
   * <p>
   * The returned resource can be used to perform a variety of common distributed systems tasks, most notable
   * of which are group membership and leader election. The {@link DistributedGroup} can be used to identify
   * members of a group or to join a group. Members of the group are guaranteed to be consistent across all
   * clients and replicas. If a member of the group is partitioned or fails, the member will be removed once
   * its session expires.
   * <p>
   * If no group exists at the given {@code key}, a new group will be created. If a group with the given key
   * already exists, a reference to the group will be returned in the {@link CompletableFuture}. The group
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the group is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided group {@link DistributedGroup.Config Config} will be used to configure the cluster-wide group.
   * If another process previously configured the group with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   *
   * @param key The resource key.
   * @param config The cluster-wide group configuration.
   * @return A completable future to be completed once the group has been created.
   */
  public CompletableFuture<DistributedGroup> getGroup(String key, DistributedGroup.Config config) {
    return getResource(key, DistributedGroup.class, config);
  }

  /**
   * Gets or creates a distributed group for managing group membership and leader elections.
   * <p>
   * The returned resource can be used to perform a variety of common distributed systems tasks, most notable
   * of which are group membership and leader election. The {@link DistributedGroup} can be used to identify
   * members of a group or to join a group. Members of the group are guaranteed to be consistent across all
   * clients and replicas. If a member of the group is partitioned or fails, the member will be removed once
   * its session expires.
   * <p>
   * If no group exists at the given {@code key}, a new group will be created. If a group with the given key
   * already exists, a reference to the group will be returned in the {@link CompletableFuture}. The group
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the group is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided group {@link DistributedGroup.Options options} will be used to configure only the local group
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedGroup.Config Config}.
   *
   * @param key The resource key.
   * @param options The local group options.
   * @return A completable future to be completed once the group has been created.
   */
  public CompletableFuture<DistributedGroup> getGroup(String key, DistributedGroup.Options options) {
    return getResource(key, DistributedGroup.class, options);
  }

  /**
   * Gets or creates a distributed group for managing group membership and leader elections.
   * <p>
   * The returned resource can be used to perform a variety of common distributed systems tasks, most notable
   * of which are group membership and leader election. The {@link DistributedGroup} can be used to identify
   * members of a group or to join a group. Members of the group are guaranteed to be consistent across all
   * clients and replicas. If a member of the group is partitioned or fails, the member will be removed once
   * its session expires.
   * <p>
   * If no group exists at the given {@code key}, a new group will be created. If a group with the given key
   * already exists, a reference to the group will be returned in the {@link CompletableFuture}. The group
   * can be accessed by any {@link AtomixClient} or {@link AtomixReplica} in the cluster.
   * <p>
   * Once the returned {@link CompletableFuture} is completed successfully, the group is guaranteed to be
   * visible by all clients and replicas in the cluster.
   * <p>
   * The provided group {@link DistributedGroup.Config Config} will be used to configure the cluster-wide group.
   * If another process previously configured the group with a different configuration, that configuration
   * will be overridden for all clients and replicas.
   * <p>
   * The provided group {@link DistributedGroup.Options options} will be used to configure only the local group
   * instance. Cluster-wide configurations can be performed by providing a {@link DistributedGroup.Config Config}.
   *
   * @param key The resource key.
   * @param config The cluster-wide group configuration.
   * @param options The local group options.
   * @return A completable future to be completed once the group has been created.
   */
  public CompletableFuture<DistributedGroup> getGroup(String key, DistributedGroup.Config config, DistributedGroup.Options options) {
    return getResource(key, DistributedGroup.class, config, options);
  }

  @Override
  public ResourceType type(Class<? extends Resource<?>> type) {
    return client.type(type);
  }

  @Override
  public CompletableFuture<Boolean> exists(String key) {
    return client.exists(key);
  }

  @Override
  public CompletableFuture<Set<String>> keys() {
    return client.keys().thenApply(this::cleanKeys);
  }

  @Override
  public <T extends Resource> CompletableFuture<Set<String>> keys(Class<? super T> type) {
    return client.keys(type).thenApply(this::cleanKeys);
  }

  @Override
  public CompletableFuture<Set<String>> keys(ResourceType type) {
    return client.keys(type).thenApply(this::cleanKeys);
  }

  /**
   * Cleans the key set.
   */
  private Set<String> cleanKeys(Set<String> keys) {
    keys.remove("");
    return keys;
  }

  @Override
  public <T extends Resource> CompletableFuture<T> getResource(String key, Class<? super T> type) {
    Assert.argNot(key.trim().length() == 0, "invalid resource key: key must be of non-zero length");
    return client.getResource(key, type, new Resource.Config(), new Resource.Options());
  }

  @Override
  public <T extends Resource> CompletableFuture<T> getResource(String key, Class<? super T> type, Resource.Config config) {
    Assert.argNot(key.trim().length() == 0, "invalid resource key: key must be of non-zero length");
    return client.getResource(key, type, config, new Resource.Options());
  }

  @Override
  public <T extends Resource> CompletableFuture<T> getResource(String key, Class<? super T> type, Resource.Options options) {
    Assert.argNot(key.trim().length() == 0, "invalid resource key: key must be of non-zero length");
    return client.getResource(key, type, new Resource.Config(), options);
  }

  @Override
  public <T extends Resource> CompletableFuture<T> getResource(String key, Class<? super T> type, Resource.Config config, Resource.Options options) {
    Assert.argNot(key.trim().length() == 0, "invalid resource key: key must be of non-zero length");
    return client.getResource(key, type, config, options);
  }

  @Override
  public <T extends Resource> CompletableFuture<T> getResource(String key, ResourceType type) {
    Assert.argNot(key.trim().length() == 0, "invalid resource key: key must be of non-zero length");
    return client.getResource(key, type, new Resource.Config(), new Resource.Options());
  }

  @Override
  public <T extends Resource> CompletableFuture<T> getResource(String key, ResourceType type, Resource.Config config) {
    Assert.argNot(key.trim().length() == 0, "invalid resource key: key must be of non-zero length");
    return client.getResource(key, type, config, new Resource.Options());
  }

  @Override
  public <T extends Resource> CompletableFuture<T> getResource(String key, ResourceType type, Resource.Options options) {
    Assert.argNot(key.trim().length() == 0, "invalid resource key: key must be of non-zero length");
    return client.getResource(key, type, new Resource.Config(), options);
  }

  @Override
  public <T extends Resource> CompletableFuture<T> getResource(String key, ResourceType type, Resource.Config config, Resource.Options options) {
    Assert.argNot(key.trim().length() == 0, "invalid resource key: key must be of non-zero length");
    return client.getResource(key, type, config, options);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
