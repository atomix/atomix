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
package io.atomix.core.map;

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.primitive.AsyncPrimitive;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Asynchronous distributed map.
 */
public interface AsyncDistributedMap<K, V> extends AsyncPrimitive {

  /**
   * Returns the number of key-value mappings in this map.  If the map contains more than <code>Integer.MAX_VALUE</code>
   * elements, returns
   * <code>Integer.MAX_VALUE</code>.
   *
   * @return the number of key-value mappings in this map
   */
  CompletableFuture<Integer> size();

  /**
   * Returns <code>true</code> if this map contains no key-value mappings.
   *
   * @return <code>true</code> if this map contains no key-value mappings
   */
  CompletableFuture<Boolean> isEmpty();

  /**
   * Returns <code>true</code> if this map contains a mapping for the specified key.  More formally, returns <code>true</code>
   * if and only if this map contains a mapping for a key <code>k</code> such that
   * <code>(key==null ? k==null : key.equals(k))</code>.  (There can be
   * at most one such mapping.)
   *
   * @param key key whose presence in this map is to be tested
   *
   * @return <code>true</code> if this map contains a mapping for the specified
   *     key
   *
   * @throws ClassCastException if the key is of an inappropriate type for this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified key is null and this map does not permit null keys (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   */
  CompletableFuture<Boolean> containsKey(K key);

  /**
   * Returns <code>true</code> if this map maps one or more keys to the specified value.  More formally, returns
   * <code>true</code> if and only if this map contains at least one mapping to a value <code>v</code> such that
   * <code>(value==null ? v==null : value.equals(v))</code>.  This operation
   * will probably require time linear in the map size for most implementations of the <code>Map</code> interface.
   *
   * @param value value whose presence in this map is to be tested
   *
   * @return <code>true</code> if this map maps one or more keys to the
   *     specified value
   *
   * @throws ClassCastException if the value is of an inappropriate type for this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified value is null and this map does not permit null values (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   */
  CompletableFuture<Boolean> containsValue(V value);

  /**
   * Returns the value to which the specified key is mapped, or {@code null} if this map contains no mapping for the
   * key.
   *
   * <p>More formally, if this map contains a mapping from a key
   * {@code k} to a value {@code v} such that {@code (key==null ? k==null : key.equals(k))}, then this method returns
   * {@code v}; otherwise it returns {@code null}.  (There can be at most one such mapping.)
   *
   * <p>If this map permits null values, then a return value of
   * {@code null} does not <i>necessarily</i> indicate that the map contains no mapping for the key; it's also possible
   * that the map explicitly maps the key to {@code null}.  The {@link #containsKey containsKey} operation may be used
   * to distinguish these two cases.
   *
   * @param key the key whose associated value is to be returned
   *
   * @return the value to which the specified key is mapped, or {@code null} if this map contains no mapping for the key
   *
   * @throws ClassCastException if the key is of an inappropriate type for this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified key is null and this map does not permit null keys (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   */
  CompletableFuture<V> get(K key);

  /**
   * Associates the specified value with the specified key in this map (optional operation).  If the map previously
   * contained a mapping for the key, the old value is replaced by the specified value.  (A map
   * <code>m</code> is said to contain a mapping for a key <code>k</code> if and only
   * if {@link #containsKey(Object) m.containsKey(k)} would return
   * <code>true</code>.)
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   *
   * @return the previous value associated with <code>key</code>, or
   *     <code>null</code> if there was no mapping for <code>key</code>.
   *     (A <code>null</code> return can also indicate that the map previously associated <code>null</code> with <code>key</code>,
   *     if the implementation supports <code>null</code> values.)
   *
   * @throws UnsupportedOperationException if the <code>put</code> operation is not supported by this map
   * @throws ClassCastException if the class of the specified key or value prevents it from being stored in this
   *     map
   * @throws NullPointerException if the specified key or value is null and this map does not permit null keys or
   *     values
   * @throws IllegalArgumentException if some property of the specified key or value prevents it from being stored
   *     in this map
   */
  CompletableFuture<V> put(K key, V value);

  /**
   * Removes the mapping for a key from this map if it is present (optional operation).   More formally, if this map
   * contains a mapping from key <code>k</code> to value <code>v</code> such that
   * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping
   * is removed.  (The map can contain at most one such mapping.)
   *
   * <p>Returns the value to which this map previously associated the key,
   * or <code>null</code> if the map contained no mapping for the key.
   *
   * <p>If this map permits null values, then a return value of
   * <code>null</code> does not <i>necessarily</i> indicate that the map
   * contained no mapping for the key; it's also possible that the map explicitly mapped the key to <code>null</code>.
   *
   * <p>The map will not contain a mapping for the specified key once the
   * call returns.
   *
   * @param key key whose mapping is to be removed from the map
   *
   * @return the previous value associated with <code>key</code>, or
   *     <code>null</code> if there was no mapping for <code>key</code>.
   *
   * @throws UnsupportedOperationException if the <code>remove</code> operation is not supported by this map
   * @throws ClassCastException if the key is of an inappropriate type for this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified key is null and this map does not permit null keys (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   */
  CompletableFuture<V> remove(K key);

  /**
   * Copies all of the mappings from the specified map to this map (optional operation).  The effect of this call is
   * equivalent to that of calling {@link #put(Object, Object) put(k, v)} on this map once for each mapping from key
   * <code>k</code> to value <code>v</code> in the specified map.  The behavior of this operation is undefined if the specified
   * map is modified while the operation is in progress.
   *
   * @param m mappings to be stored in this map
   *
   * @throws UnsupportedOperationException if the <code>putAll</code> operation is not supported by this map
   * @throws ClassCastException if the class of a key or value in the specified map prevents it from being stored in
   *     this map
   * @throws NullPointerException if the specified map is null, or if this map does not permit null keys or values,
   *     and the specified map contains null keys or values
   * @throws IllegalArgumentException if some property of a key or value in the specified map prevents it from being
   *     stored in this map
   */
  CompletableFuture<Void> putAll(Map<? extends K, ? extends V> m);

  /**
   * Removes all of the mappings from this map (optional operation). The map will be empty after this call returns.
   *
   * @throws UnsupportedOperationException if the <code>clear</code> operation is not supported by this map
   */
  CompletableFuture<Void> clear();

  /**
   * Returns a {@link Set} view of the keys contained in this map. The set is backed by the map, so changes to the map
   * are reflected in the set, and vice-versa.  If the map is modified while an iteration over the set is in progress
   * (except through the iterator's own <code>remove</code> operation), the results of the iteration are undefined.  The set
   * supports element removal, which removes the corresponding mapping from the map, via the
   * <code>Iterator.remove</code>, <code>Set.remove</code>,
   * <code>removeAll</code>, <code>retainAll</code>, and <code>clear</code>
   * operations.  It does not support the <code>add</code> or <code>addAll</code> operations.
   *
   * @return a set view of the keys contained in this map
   */
  AsyncDistributedSet<K> keySet();

  /**
   * Returns a {@link Collection} view of the values contained in this map. The collection is backed by the map, so
   * changes to the map are reflected in the collection, and vice-versa.  If the map is modified while an iteration over
   * the collection is in progress (except through the iterator's own <code>remove</code> operation), the results of the
   * iteration are undefined.  The collection supports element removal, which removes the corresponding mapping from the
   * map, via the <code>Iterator.remove</code>,
   * <code>Collection.remove</code>, <code>removeAll</code>,
   * <code>retainAll</code> and <code>clear</code> operations.  It does not
   * support the <code>add</code> or <code>addAll</code> operations.
   *
   * @return a collection view of the values contained in this map
   */
  AsyncDistributedCollection<V> values();

  /**
   * Returns a {@link Set} view of the mappings contained in this map. The set is backed by the map, so changes to the
   * map are reflected in the set, and vice-versa.  If the map is modified while an iteration over the set is in
   * progress (except through the iterator's own <code>remove</code> operation, or through the
   * <code>setValue</code> operation on a map entry returned by the
   * iterator) the results of the iteration are undefined.  The set supports element removal, which removes the
   * corresponding mapping from the map, via the <code>Iterator.remove</code>,
   * <code>Set.remove</code>, <code>removeAll</code>, <code>retainAll</code> and
   * <code>clear</code> operations.  It does not support the
   * <code>add</code> or <code>addAll</code> operations.
   *
   * @return a set view of the mappings contained in this map
   */
  AsyncDistributedSet<Map.Entry<K, V>> entrySet();

  /**
   * Returns the value to which the specified key is mapped, or {@code defaultValue} if this map contains no mapping for
   * the key.
   *
   * @param key the key whose associated value is to be returned
   * @param defaultValue the default mapping of the key
   *
   * @return the value to which the specified key is mapped, or {@code defaultValue} if this map contains no mapping for
   *     the key
   *
   * @throws ClassCastException if the key is of an inappropriate type for this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified key is null and this map does not permit null keys (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   */
  CompletableFuture<V> getOrDefault(K key, V defaultValue);

  /**
   * If the specified key is not already associated with a value (or is mapped to {@code null}) associates it with the
   * given value and returns {@code null}, else returns the current value.
   *
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   *
   * @return the previous value associated with the specified key, or {@code null} if there was no mapping for the key.
   *     (A {@code null} return can also indicate that the map previously associated {@code null} with the key, if the
   *     implementation supports null values.)
   *
   * @throws UnsupportedOperationException if the {@code put} operation is not supported by this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws ClassCastException if the key or value is of an inappropriate type for this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified key or value is null, and this map does not permit null keys or
   *     values (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws IllegalArgumentException if some property of the specified key or value prevents it from being stored
   *     in this map (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   */
  CompletableFuture<V> putIfAbsent(K key, V value);

  /**
   * Removes the entry for the specified key only if it is currently mapped to the specified value.
   *
   * @param key key with which the specified value is associated
   * @param value value expected to be associated with the specified key
   *
   * @return {@code true} if the value was removed
   *
   * @throws UnsupportedOperationException if the {@code remove} operation is not supported by this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws ClassCastException if the key or value is of an inappropriate type for this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified key or value is null, and this map does not permit null keys or
   *     values (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   */
  CompletableFuture<Boolean> remove(K key, V value);

  /**
   * Replaces the entry for the specified key only if currently mapped to the specified value.
   *
   * @param key key with which the specified value is associated
   * @param oldValue value expected to be associated with the specified key
   * @param newValue value to be associated with the specified key
   *
   * @return {@code true} if the value was replaced
   *
   * @throws UnsupportedOperationException if the {@code put} operation is not supported by this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws ClassCastException if the class of a specified key or value prevents it from being stored in this map
   * @throws NullPointerException if a specified key or newValue is null, and this map does not permit null keys or
   *     values
   * @throws NullPointerException if oldValue is null and this map does not permit null values (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws IllegalArgumentException if some property of a specified key or value prevents it from being stored in
   *     this map
   */
  CompletableFuture<Boolean> replace(K key, V oldValue, V newValue);

  /**
   * Replaces the entry for the specified key only if it is currently mapped to some value.
   *
   * @param key key with which the specified value is associated
   * @param value value to be associated with the specified key
   *
   * @return the previous value associated with the specified key, or {@code null} if there was no mapping for the key.
   *     (A {@code null} return can also indicate that the map previously associated {@code null} with the key, if the
   *     implementation supports null values.)
   *
   * @throws UnsupportedOperationException if the {@code put} operation is not supported by this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws ClassCastException if the class of the specified key or value prevents it from being stored in this map
   *     (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws NullPointerException if the specified key or value is null, and this map does not permit null keys or
   *     values
   * @throws IllegalArgumentException if some property of the specified key or value prevents it from being stored
   *     in this map
   */
  CompletableFuture<V> replace(K key, V value);

  /**
   * If the specified key is not already associated with a value (or is mapped to {@code null}), attempts to compute its
   * value using the given mapping function and enters it into this map unless {@code null}.
   *
   * <p>If the function returns {@code null} no mapping is recorded. If
   * the function itself throws an (unchecked) exception, the exception is rethrown, and no mapping is recorded.  The
   * most common usage is to construct a new object serving as an initial mapped value or memoized result, as in:
   *
   * <pre> {@code
   * map.computeIfAbsent(key, k -> new Value(f(k)));
   * }</pre>
   *
   * <p>Or to implement a multi-value map, {@code Map<K,Collection<V>>},
   * supporting multiple values per key:
   *
   * <pre> {@code
   * map.computeIfAbsent(key, k -> new HashSet<V>()).add(v);
   * }</pre>
   *
   * @param key key with which the specified value is to be associated
   * @param mappingFunction the function to compute a value
   *
   * @return the current (existing or computed) value associated with the specified key, or null if the computed value
   *     is null
   *
   * @throws NullPointerException if the specified key is null and this map does not support null keys, or the
   *     mappingFunction is null
   * @throws UnsupportedOperationException if the {@code put} operation is not supported by this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws ClassCastException if the class of the specified key or value prevents it from being stored in this map
   *     (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   */
  CompletableFuture<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction);

  /**
   * If the value for the specified key is present and non-null, attempts to compute a new mapping given the key and its
   * current mapped value.
   *
   * <p>If the function returns {@code null}, the mapping is removed.  If the
   * function itself throws an (unchecked) exception, the exception is rethrown, and the current mapping is left
   * unchanged.
   *
   * @param key key with which the specified value is to be associated
   * @param remappingFunction the function to compute a value
   *
   * @return the new value associated with the specified key, or null if none
   *
   * @throws NullPointerException if the specified key is null and this map does not support null keys, or the
   *     remappingFunction is null
   * @throws UnsupportedOperationException if the {@code put} operation is not supported by this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws ClassCastException if the class of the specified key or value prevents it from being stored in this map
   *     (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   */
  CompletableFuture<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

  /**
   * Attempts to compute a mapping for the specified key and its current mapped value (or {@code null} if there is no
   * current mapping). For example, to either create or append a {@code String} msg to a value mapping:
   *
   * <p>If the function returns {@code null}, the mapping is removed (or
   * remains absent if initially absent).  If the function itself throws an (unchecked) exception, the exception is
   * rethrown, and the current mapping is left unchanged.
   *
   * @param key key with which the specified value is to be associated
   * @param remappingFunction the function to compute a value
   *
   * @return the new value associated with the specified key, or null if none
   *
   * @throws NullPointerException if the specified key is null and this map does not support null keys, or the
   *     remappingFunction is null
   * @throws UnsupportedOperationException if the {@code put} operation is not supported by this map (<a
   *     href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   * @throws ClassCastException if the class of the specified key or value prevents it from being stored in this map
   *     (<a href="{@docRoot}/java/util/Collection.html#optional-restrictions">optional</a>)
   */
  CompletableFuture<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

  /**
   * Registers the specified listener to be notified whenever the map is updated.
   *
   * @param listener listener to notify about map events
   *
   * @return future that will be completed when the operation finishes
   */
  default CompletableFuture<Void> addListener(MapEventListener<K, V> listener) {
    return addListener(listener, MoreExecutors.directExecutor());
  }

  /**
   * Registers the specified listener to be notified whenever the map is updated.
   *
   * @param listener listener to notify about map events
   * @param executor executor to use for handling incoming map events
   *
   * @return future that will be completed when the operation finishes
   */
  CompletableFuture<Void> addListener(MapEventListener<K, V> listener, Executor executor);

  /**
   * Unregisters the specified listener such that it will no longer receive map change notifications.
   *
   * @param listener listener to unregister
   *
   * @return future that will be completed when the operation finishes
   */
  CompletableFuture<Void> removeListener(MapEventListener<K, V> listener);

  @Override
  default DistributedMap<K, V> sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  DistributedMap<K, V> sync(Duration operationTimeout);
}
