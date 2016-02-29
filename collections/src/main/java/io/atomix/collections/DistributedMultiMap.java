/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.collections;

import io.atomix.collections.state.MultiMapCommands;
import io.atomix.collections.state.MultiMapState;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.ReadConsistency;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceTypeInfo;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Stores a map of keys to multiple values.
 * <p>
 * The multi-map resource stores a map of keys that allows multiple values to be associated with each key.
 * Multimap entries are stored in memory on each stateful node and backed by disk, thus the size of a multi-map
 * is limited by the available memory on the smallest node in the cluster.
 * <p>
 * To create a multimap, use the {@code getMultiMap} factory method on an {@code Atomix} instance:
 * <pre>
 *   {@code
 *   DistributedMultiMap<String, String> multiMap = atomix.getMultiMap("foo").get();
 *   }
 * </pre>
 * The multi-map interface closely simulates that of {@link java.util.Map} except that values are {@link Collection}s
 * rather than {@code V}.
 * <h3>Value order</h3>
 * By default, the values associated with each key are stored in insertion order. However, this behavior can
 * be configured via the {@link io.atomix.collections.DistributedMultiMap.Config map configuration} by setting
 * the value {@link Order}. To set the order of values in a multi-map, create a configuration and provide the
 * configuration at the creation of the map.
 * <pre>
 *   {@code
 *   DistributedMultiMap.Config config = DistributedMultiMap.config()
 *     .withValueOrder(DistributedMultiMap.Order.NATURAL);
 *   DistributedMultiMap<String, String> multiMap = atomix.getMultiMap("foo", config).get();
 *   }
 * </pre>
 * Multi-maps support relaxed consistency levels for some read operations line {@link #size(ReadConsistency)}
 * and {@link #containsKey(Object, ReadConsistency)}. By default, read operations on a queue are linearizable
 * but require some level of communication between nodes.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-12, stateMachine=MultiMapState.class, typeResolver=MultiMapCommands.TypeResolver.class)
public class DistributedMultiMap<K, V> extends Resource<DistributedMultiMap<K, V>> {

  /**
   * Multimap configuration.
   */
  public static class Config extends Resource.Config {

    public Config() {
    }

    public Config(Properties defaults) {
      super(defaults);
    }

    /**
     * Sets the map value order.
     *
     * @param order The map value order.
     * @return The map configuration.
     */
    public Config withValueOrder(Order order) {
      setProperty("order", order.name().toLowerCase());
      return this;
    }

    /**
     * Returns the map value order.
     *
     * @return The map value order.
     */
    public Order getValueOrder() {
      return Order.valueOf(getProperty("order", Order.INSERT.name().toLowerCase()).toUpperCase());
    }
  }

  /**
   * Represents the order of values in a multimap.
   */
  public enum Order {

    /**
     * Indicates that values should be stored in natural order.
     */
    NATURAL,

    /**
     * Indicates that values should be stored in insertion order.
     */
    INSERT,

    /**
     * Indicates that no order is required for values.
     */
    NONE

  }

  public DistributedMultiMap(CopycatClient client, Properties config, Properties options) {
    super(client, config, options);
  }

  @Override
  public Resource.Config config() {
    return new Config(super.config());
  }

  /**
   * Checks whether the map is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return submit(new MultiMapCommands.IsEmpty());
  }

  /**
   * Checks whether the map is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  public CompletableFuture<Boolean> isEmpty(ReadConsistency consistency) {
    return submit(new MultiMapCommands.IsEmpty(), consistency);
  }

  /**
   * Gets the number of key-value pairs in the map.
   *
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size() {
    return submit(new MultiMapCommands.Size());
  }

  /**
   * Gets the number of key-value pairs in the map.
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size(ReadConsistency consistency) {
    return submit(new MultiMapCommands.Size(), consistency);
  }

  /**
   * Gets the number of values for the given key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size(K key) {
    return submit(new MultiMapCommands.Size(key));
  }

  /**
   * Gets the number of values for the given key.
   *
   * @param key The key to check.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size(K key, ReadConsistency consistency) {
    return submit(new MultiMapCommands.Size(key), consistency);
  }

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsKey(K key) {
    return submit(new MultiMapCommands.ContainsKey(key));
  }

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsKey(K key, ReadConsistency consistency) {
    return submit(new MultiMapCommands.ContainsKey(key), consistency);
  }

  /**
   * Checks whether the map contains an entry.
   *
   * @param key The key to check.
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsEntry(K key, V value) {
    return submit(new MultiMapCommands.ContainsEntry(key, value));
  }

  /**
   * Checks whether the map contains an entry.
   *
   * @param key The key to check.
   * @param value The value to check.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsEntry(K key, V value, ReadConsistency consistency) {
    return submit(new MultiMapCommands.ContainsEntry(key, value), consistency);
  }

  /**
   * Checks whether the map contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsValue(V value) {
    return submit(new MultiMapCommands.ContainsValue(value));
  }

  /**
   * Checks whether the map contains a value.
   *
   * @param value The value to check.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsValue(V value, ReadConsistency consistency) {
    return submit(new MultiMapCommands.ContainsValue(value), consistency);
  }

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Collection<V>> get(K key) {
    return submit(new MultiMapCommands.Get(key))
      .thenApply(result -> result);
  }

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Collection<V>> get(K key, ReadConsistency consistency) {
    return submit(new MultiMapCommands.Get(key), consistency)
      .thenApply(result -> result);
  }

  /**
   * Puts a value in the map.
   *
   * @param key   The key to set.
   * @param value The value to set.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> put(K key, V value) {
    return submit(new MultiMapCommands.Put(key, value))
      .thenApply(result -> result);
  }

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The duration after which to expire the key.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> put(K key, V value, Duration ttl) {
    return submit(new MultiMapCommands.Put(key, value, ttl.toMillis()));
  }

  /**
   * Removes a value from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Collection<V>> remove(Object key) {
    return submit(new MultiMapCommands.Remove(key))
      .thenApply(result -> (Collection) result);
  }

  /**
   * Removes a key and value from the map.
   *
   * @param key   The key to remove.
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(Object key, Object value) {
    return submit(new MultiMapCommands.Remove(key, value))
      .thenApply(result -> (boolean) result);
  }

  /**
   * Removes all instances of a value from the map.
   *
   * @param value The value to remove.
   * @return A completable future to be completed once the value has been removed.
   */
  public CompletableFuture<Void> removeValue(Object value) {
    return submit(new MultiMapCommands.RemoveValue(value));
  }

  /**
   * Removes all entries from the map.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return submit(new MultiMapCommands.Clear());
  }

}
