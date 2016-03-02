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

import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.collections.state.MapCommands;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.ReadConsistency;
import io.atomix.resource.ResourceTypeInfo;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Stores a map of keys to values.
 * <p>
 * The distributed map stores a map of keys and values via an interface similar to that of {@link Map}.
 * Map entries are stored in memory on each stateful node and backed by disk, thus the size of the map is
 * limited to the available memory on the smallest node in the cluster. This map requires non-null keys but
 * supports {@code null} values. All keys and values must be serializable by a
 * {@link io.atomix.catalyst.serializer.Serializer}. Serializable types include implementations of
 * {@link java.io.Serializable} or {@link io.atomix.catalyst.serializer.CatalystSerializable}. See the
 * serialization API for more information.
 * <p>
 * A {@code DistributedMap} can be created either via the {@code Atomix} API or by wrapping a {@link CopycatClient}
 * directly. To create a map via the Atomix API, use the {@code getMap} factory method:
 * <pre>
 *   {@code
 *   DistributedMap<String, String> map = atomix.getMap("foo").get();
 *   }
 * </pre>
 * Maps are distributed and are referenced by the map name. If a value is {@link #put(Object, Object)} in
 * a map on one node, that value is immediately available for {@link #get(Object) reading} by any other node
 * in the cluster by operating on the same map.
 * <p>
 * In addition to supporting normal {@link java.util.Map} methods, this implementation supports values
 * with TTLs. When a key is set with a TTL, the value will expire and be automatically evicted from the map
 * some time after the TTL.
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-11, factory=DistributedMapFactory.class)
public class DistributedMap<K, V> extends AbstractResource<DistributedMap<K, V>> {

  public DistributedMap(CopycatClient client) {
    this(client, new Options());
  }

  public DistributedMap(CopycatClient client, Properties options) {
    super(client, options);
  }

  @Override
  protected void registerTypes(SerializerRegistry registry) {
    new MapCommands.TypeResolver().resolve(registry);
  }

  /**
   * Returns {@code true} if the map is empty.
   * <p>
   * Note that depending on the configured {@link ReadConsistency} of the map instance, empty checks
   * may return stale results. To perform a fully consistent empty check, configure the map with
   * {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.with(Consistency.ATOMIC).isEmpty().thenAccept(isEmpty -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   if (map.isEmpty().get()) {
   *     ...
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.isEmpty().thenAccept(isEmpty -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return submit(new MapCommands.IsEmpty());
  }

  /**
   * Returns {@code true} if the map is empty.
   * <p>
   * Note that depending on the {@link ReadConsistency}, empty checks may return stale results. To perform a fully
   * consistent empty check, use {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.isEmpty(ReadConsistency.ATOMIC).thenAccept(isEmpty -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   if (map.isEmpty(ReadConsistency.ATOMIC).get()) {
   *     ...
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.isEmpty(ReadConsistency.ATOMIC).thenAccept(isEmpty -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  public CompletableFuture<Boolean> isEmpty(ReadConsistency consistency) {
    return submit(new MapCommands.IsEmpty(), consistency);
  }

  /**
   * Gets the number of key-value pairs in the map.
   * <p>
   * Note that depending on the configured {@link ReadConsistency} of the map instance, size checks
   * may return stale results. To perform a fully consistent size check, configure the map with
   * {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.with(Consistency.ATOMIC).size().thenAccept(size -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   int size = map.size().get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.size().thenAccept(size -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size() {
    return submit(new MapCommands.Size());
  }

  /**
   * Gets the number of key-value pairs in the map.
   * <p>
   * Note that depending on the {@link ReadConsistency}, size checks may return stale results. To perform a
   * fully consistent size check, use {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.size(ReadConsistency.ATOMIC).thenAccept(size -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   int size = map.size(ReadConsistency.ATOMIC).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.size(ReadConsistency.ATOMIC).thenAccept(size -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size(ReadConsistency consistency) {
    return submit(new MapCommands.Size(), consistency);
  }

  /**
   * Returns {@code true} if the given key is present in the map.
   * <p>
   * Note that depending on the configured {@link ReadConsistency} of the map instance, checks
   * may return stale results. To perform a fully consistent check, configure the map with
   * {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.with(Consistency.ATOMIC).containsKey("foo").thenAccept(contains -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   if (map.containsKey("foo").get()) {
   *     ...
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.containsKey("foo").thenAccept(contains -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to check.
   * @return A completable future to be completed with a boolean indicating whether {@code key} is present in the map.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  public CompletableFuture<Boolean> containsKey(Object key) {
    return submit(new MapCommands.ContainsKey(key));
  }

  /**
   * Returns {@code true} if the given key is present in the map.
   * <p>
   * Note that depending on the {@link ReadConsistency}, checks may return stale results. To perform a fully
   * consistent check, use {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.containsKey("foo", ReadConsistency.ATOMIC).thenAccept(contains -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   if (map.containsKey("foo", ReadConsistency.ATOMIC).get()) {
   *     ...
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.containsKey("foo", ReadConsistency.ATOMIC).thenAccept(contains -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to check.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with a boolean indicating whether {@code key} is present in the map.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  public CompletableFuture<Boolean> containsKey(Object key, ReadConsistency consistency) {
    return submit(new MapCommands.ContainsKey(key), consistency);
  }

  /**
   * Returns {@code true} if the map contains a key with the given value.
   * <p>
   * Note that depending on the configured {@link ReadConsistency} of the map instance, checks
   * may return stale results. To perform a fully consistent check, configure the map with
   * {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.with(Consistency.ATOMIC).containsValue("foo").thenAccept(contains -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   if (map.containsValue("foo").get()) {
   *     ...
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.containsValue("foo").thenAccept(contains -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param value The value for which to search keys.
   * @return A completable future to be completed with a boolean indicating whether {@code key} is present in the map.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  public CompletableFuture<Boolean> containsValue(Object value) {
    return submit(new MapCommands.ContainsValue(value));
  }

  /**
   * Returns {@code true} if the map contains a key with the given value.
   * <p>
   * Note that depending on the {@link ReadConsistency}, checks may return stale results. To perform a fully
   * consistent check, use {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.containsValue("foo", ReadConsistency.ATOMIC).thenAccept(contains -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   if (map.containsValue("foo", ReadConsistency.ATOMIC).get()) {
   *     ...
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.containsValue("foo", ReadConsistency.ATOMIC).thenAccept(contains -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param value The value for which to search keys.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with a boolean indicating whether {@code key} is present in the map.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  public CompletableFuture<Boolean> containsValue(Object value, ReadConsistency consistency) {
    return submit(new MapCommands.ContainsValue(value), consistency);
  }

  /**
   * Gets a value from the map.
   * <p>
   * Note that depending on the configured {@link ReadConsistency} of the map instance, queries
   * may return stale results. To perform a fully consistent query, configure the map with
   * {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.with(Consistency.ATOMIC).get("key").thenAccept(value -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   String value = map.get("key").get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.get("key").thenAccept(value -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> get(Object key) {
    return submit(new MapCommands.Get(key))
      .thenApply(result -> (V) result);
  }

  /**
   * Gets a value from the map.
   * <p>
   * Note that depending on the {@link ReadConsistency}, queries may return stale results. To perform a fully
   * consistent query, use {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.get("key", ReadConsistency.ATOMIC).thenAccept(value -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   String value = map.get("key", ReadConsistency.ATOMIC).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.get("key", ReadConsistency.ATOMIC).thenAccept(value -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to get.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> get(Object key, ReadConsistency consistency) {
    return submit(new MapCommands.Get(key), consistency)
      .thenApply(result -> (V) result);
  }

  /**
   * Gets the value of {@code key} or returns the given default value if {@code key} does not exist.
   * <p>
   * If no value for the given {@code key} is present in the map, the returned {@link CompletableFuture} will
   * be completed {@code null}. If a value is present, the returned future will be completed with that value.
   * <p>
   * Note that depending on the configured {@link ReadConsistency} of the map instance, queries
   * may return stale results. To perform a fully consistent query, configure the map with
   * {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.with(Consistency.ATOMIC).getOrDefault("key", "Hello world!").thenAccept(value -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   String valueOrDefault = map.getOrDefault("key", "Hello world!").get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.getOrDefault("key", "Hello world!").thenAccept(valueOrDefault -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to get.
   * @param defaultValue The default value to return if the key does not exist.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> getOrDefault(Object key, V defaultValue) {
    return submit(new MapCommands.GetOrDefault(key, defaultValue))
      .thenApply(result -> (V) result);
  }

  /**
   * Gets the value of {@code key} or returns the given default value if {@code key} does not exist.
   * <p>
   * If no value for the given {@code key} is present in the map, the returned {@link CompletableFuture} will
   * be completed {@code null}. If a value is present, the returned future will be completed with that value.
   * <p>
   * Note that depending on the {@link ReadConsistency}, queries may return stale results. To perform a fully
   * consistent query, use {@link ReadConsistency#ATOMIC} consistency (the default).
   * <pre>
   *   {@code
   *   map.getOrDefault("key", "Hello world!", ReadConsistency.ATOMIC).thenAccept(value -> {
   *     ...
   *   });
   *   }
   * </pre>
   * For better performance with potentially stale results, use a lower consistency level. See the
   * {@link ReadConsistency} documentation for specific consistency guarantees.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} method to block the calling thread:
   * <pre>
   *   {@code
   *   String valueOrDefault = map.getOrDefault("key", "Hello world!", ReadConsistency.ATOMIC).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.getOrDefault("key", "Hello world!", ReadConsistency.ATOMIC).thenAccept(valueOrDefault -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to get.
   * @param defaultValue The default value to return if the key does not exist.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> getOrDefault(Object key, V defaultValue, ReadConsistency consistency) {
    return submit(new MapCommands.GetOrDefault(key, defaultValue), consistency)
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map for the given {@code key}.
   * <p>
   * Any previous value associated with the given {@code key} will be overridden. Additionally, if the previous value
   * was set with a TTL, the TTL will be cancelled and this value will be set with no TTL.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   String oldValue = map.put("key", "Hello world!").get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.put("key", "Hello world!").thenAccept(oldValue -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key   The key to set.
   * @param value The value to set.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value) {
    return submit(new MapCommands.Put(key, value))
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map with a time-to-live for the given {@code key}.
   * <p>
   * Any previous value associated with the given {@code key} will be overridden. Additionally, if the previous value
   * was set with a TTL, the TTL will be cancelled and this value's TTL will be set.
   * <p>
   * The {@code value} will remain in the map until the provided {@link Duration} of time has elapsed or it is overridden
   * by a more recent put operation. Note that the provided {@code ttl} should only be considered an estimate of time.
   * Values may be evicted at some arbitrary point after the provided duration has elapsed, but never before.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   String oldValue = map.put("key", "Hello world!", Duration.ofSeconds(10)).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.put("key", "Hello world!", Duration.ofSeconds(10)).thenAccept(oldValue -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The duration after which to expire the key.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, Duration ttl) {
    return submit(new MapCommands.Put(key, value, ttl.toMillis()))
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map if the given {@code key} does not exist.
   * <p>
   * If the given {@code key} is not already present in the map, the key will be set to the given {@code value}.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   String oldValue = map.put("key", "Hello world!").get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.put("key", "Hello world!").thenAccept(oldValue -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return submit(new MapCommands.PutIfAbsent(key, value))
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value with a time-to-live in the map if the given {@code key} does not exist.
   * <p>
   * If the given {@code key} is not already present in the map, the key will be set to the given {@code value}.
   * <p>
   * The {@code value} will remain in the map until the provided {@link Duration} of time has elapsed or it is overridden
   * by a more recent put operation. Note that the provided {@code ttl} should only be considered an estimate of time.
   * Values may be evicted at some arbitrary point after the provided duration has elapsed, but never before.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   String oldValue = map.put("key", "Hello world!", Duration.ofSeconds(10)).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.put("key", "Hello world!", Duration.ofSeconds(10)).thenAccept(oldValue -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @param ttl The time to live duration.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value, Duration ttl) {
    return submit(new MapCommands.PutIfAbsent(key, value, ttl.toMillis())).thenApply(result -> (V) result);
  }

  /**
   * Removes a the value for the given {@code key} from the map.
   * <p>
   * If no value for the given {@code key} is present in the map, the returned {@link CompletableFuture} will
   * be completed {@code null}. If a value is present, that value will be removed from the distributed map and the
   * returned future will be completed with that value. If the previous value was set with a TTL, the TTL will
   * be cancelled.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   String oldValue = map.remove("key").get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.remove("key").thenAccept(oldValue -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> remove(Object key) {
    return submit(new MapCommands.Remove(key))
      .thenApply(result -> (V) result);
  }

  /**
   * Removes the given {@code key} from the map if its value matches the given {@code value}.
   * <p>
   * If no value for the given {@code key} is present in the map or if the value doesn't match the provided {@code value},
   * the returned {@link CompletableFuture} will be completed {@code false}. If a value is present and matches {@code value},
   * that value will be removed from the distributed map and the returned future will be completed {@code true}. If the
   * previous value was set with a TTL, the TTL will be cancelled.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   if (map.remove("key", "Hello world!").get()) {
   *     ...
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.remove("key", "Hello world!").thenAccept(removed -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key   The key to remove.
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  public CompletableFuture<Boolean> remove(K key, V value) {
    return submit(new MapCommands.RemoveIfPresent(key, value));
  }

  /**
   * Replaces a value in the map if the {@code key} exists.
   * <p>
   * If the given {@code key} is not already present in the map, no change will be made and {@code null} will be returned.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   String oldValue = map.replace("key", "Hello world!").get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.replace("key", "Hello world!").thenAccept(oldValue -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to replace.
   * @param value The value with which to replace the key if it exists.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> replace(K key, V value) {
    return submit(new MapCommands.Replace(key, value))
      .thenApply(result -> (V) result);
  }

  /**
   * Replaces a value in the map if the {@code key} exist.
   * <p>
   * If the given {@code key} is not already present in the map, no change will be made and {@code null} will be returned.
   * <p>
   * If the value is successfully replaced, the {@code value} will remain in the map until the provided {@link Duration}
   * of time has elapsed or it is overridden by a more recent put operation. Note that the provided {@code ttl} should
   * only be considered an estimate of time. Values may be evicted at some arbitrary point after the provided duration
   * has elapsed, but never before.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   String oldValue = map.replace("key", "Hello world!", Duration.ofSeconds(10)).get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.replace("key", "Hello world!", Duration.ofSeconds(10)).thenAccept(oldValue -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to replace.
   * @param value The value with which to replace the key if it exists.
   * @param ttl The duration after which to expire the key/value.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> replace(K key, V value, Duration ttl) {
    return submit(new MapCommands.Replace(key, value, ttl.toMillis())).thenApply(result -> (V) result);
  }

  /**
   * Replaces a value in the map.
   * <p>
   * If the given {@code key} is not already present in the map, no change will be made and {@code null} will be returned.
   * If the key is present and its value matches {@code oldValue}, it will be updated with {@code newValue}.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   if (map.replace("key", "Hello world!", "Hello world again!").get()) {
   *     ...
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.replace("key", "Hello world!", "Hello world again!").thenAccept(replaced -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to replace.
   * @param oldValue The value to check.
   * @param newValue The value to replace.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return submit(new MapCommands.ReplaceIfPresent(key, oldValue, newValue));
  }

  /**
   * Replaces a value in the map with a time-to-live.
   * <p>
   * If the given {@code key} is not already present in the map, no change will be made and {@code null} will be returned.
   * If the key is present and its value matches {@code oldValue}, it will be updated with {@code newValue}.
   * <p>
   * If the value is successfully replaced, the {@code value} will remain in the map until the provided {@link Duration}
   * of time has elapsed or it is overridden by a more recent put operation. Note that the provided {@code ttl} should
   * only be considered an estimate of time. Values may be evicted at some arbitrary point after the provided duration
   * has elapsed, but never before.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   if (map.replace("key", "Hello world!", "Hello world again!", Duration.ofSeconds(10)).get()) {
   *     ...
   *   }
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.replace("key", "Hello world!", "Hello world again!", Duration.ofSeconds(10)).thenAccept(oldValue -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @param key The key to replace.
   * @param oldValue The value to check.
   * @param newValue The value to replace.
   * @param ttl The duration after which to expire the key/value.
   * @return A completable future to be completed with the result once complete.
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue, Duration ttl) {
    return submit(new MapCommands.ReplaceIfPresent(key, oldValue, newValue, ttl.toMillis()));
  }

  /**
   * Reads the set of all keys in the map.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   Set<String> keys = map.keySet().get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.keySet().thenAccept(keys -> {
   *     keys.forEach(key -> ...);
   *   });
   *   map.replace("key", "Hello world!", "Hello world again!", Duration.ofSeconds(10)).thenAccept(oldValue -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Set<K>> keySet() {
    return submit(new MapCommands.KeySet()).thenApply(keys -> (Set<K>) keys);
  }

  /**
   * Reads the collection of all values in the map.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   Set<String> keys = map.keySet().get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.keySet().thenAccept(keys -> {
   *     keys.forEach(key -> ...);
   *   });
   *   map.replace("key", "Hello world!", "Hello world again!", Duration.ofSeconds(10)).thenAccept(oldValue -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Collection<V>> values() {
    return submit(new MapCommands.Values()).thenApply(values -> (Collection<V>) values);
  }

  /**
   * Reads the set of all entries in the map.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   Set<String> keys = map.keySet().get();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.keySet().thenAccept(keys -> {
   *     keys.forEach(key -> ...);
   *   });
   *   map.replace("key", "Hello world!", "Hello world again!", Duration.ofSeconds(10)).thenAccept(oldValue -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Set<Map.Entry<K, V>>> entrySet() {
    return submit(new MapCommands.EntrySet()).thenApply(entries -> (Set<Map.Entry<K, V>>) entries);
  }

  /**
   * Removes all entries from the map.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   map.clear().join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the operation is complete in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   map.clear().thenRun(() -> {
   *     ...
   *   });
   *   }
   * </pre>
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return submit(new MapCommands.Clear());
  }

}
