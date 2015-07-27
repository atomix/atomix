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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.PersistenceLevel;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.Stateful;
import net.kuujo.copycat.collections.state.MapCommands;
import net.kuujo.copycat.collections.state.MapState;
import net.kuujo.copycat.raft.ConsistencyLevel;
import net.kuujo.copycat.raft.Raft;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous map.
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(MapState.class)
public class DistributedMap<K, V> extends Resource implements AsyncMap<K, V> {

  public DistributedMap(Raft protocol) {
    super(protocol);
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return submit(MapCommands.IsEmpty.builder().build());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty(ConsistencyLevel consistency) {
    return submit(MapCommands.IsEmpty.builder().withConsistency(consistency).build());
  }

  @Override
  public CompletableFuture<Integer> size() {
    return submit(MapCommands.Size.builder().build());
  }

  @Override
  public CompletableFuture<Integer> size(ConsistencyLevel consistency) {
    return submit(MapCommands.Size.builder().withConsistency(consistency).build());
  }

  @Override
  public CompletableFuture<Boolean> containsKey(Object key) {
    return submit(MapCommands.ContainsKey.builder()
      .withKey(key)
      .build());
  }

  @Override
  public CompletableFuture<Boolean> containsKey(Object key, ConsistencyLevel consistency) {
    return submit(MapCommands.ContainsKey.builder()
      .withKey(key)
      .withConsistency(consistency)
      .build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> get(Object key) {
    return submit(MapCommands.Get.builder()
      .withKey(key)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> get(Object key, ConsistencyLevel consistency) {
    return submit(MapCommands.Get.builder()
      .withKey(key)
      .withConsistency(consistency)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value) {
    return submit(MapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, PersistenceLevel persistence) {
    return submit(MapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .withPersistence(persistence)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, long ttl) {
    return submit(MapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, long ttl, PersistenceLevel persistence) {
    return submit(MapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl)
      .withPersistence(persistence)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, long ttl, TimeUnit unit) {
    return submit(MapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl, unit)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, long ttl, TimeUnit unit, PersistenceLevel persistence) {
    return submit(MapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl, unit)
      .withPersistence(persistence)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> remove(Object key) {
    return submit(MapCommands.Remove.builder()
      .withKey(key)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> getOrDefault(Object key, V defaultValue) {
    return submit(MapCommands.GetOrDefault.builder()
      .withKey(key)
      .withDefaultValue(defaultValue)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> getOrDefault(Object key, V defaultValue, ConsistencyLevel consistency) {
    return submit(MapCommands.GetOrDefault.builder()
      .withKey(key)
      .withDefaultValue(defaultValue)
      .withConsistency(consistency)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return submit(MapCommands.PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value, long ttl) {
    return submit(MapCommands.PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value, long ttl, PersistenceLevel persistence) {
    return submit(MapCommands.PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl)
      .withPersistence(persistence)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value, long ttl, TimeUnit unit) {
    return submit(MapCommands.PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl, unit)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value, long ttl, TimeUnit unit, PersistenceLevel persistence) {
    return submit(MapCommands.PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl, unit)
      .withPersistence(persistence)
      .build())
      .thenApply(result -> (V) result);
  }

  @Override
  public CompletableFuture<Boolean> remove(Object key, Object value) {
    return submit(MapCommands.Remove.builder()
      .withKey(key)
      .withValue(value)
      .build())
      .thenApply(result -> (boolean) result);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return submit(MapCommands.Clear.builder().build());
  }

}
