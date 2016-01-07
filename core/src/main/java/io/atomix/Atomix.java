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

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.collections.DistributedMap;
import io.atomix.collections.DistributedMultiMap;
import io.atomix.collections.DistributedQueue;
import io.atomix.collections.DistributedSet;
import io.atomix.coordination.DistributedLock;
import io.atomix.coordination.DistributedMembershipGroup;
import io.atomix.manager.ResourceClient;
import io.atomix.manager.ResourceManager;
import io.atomix.messaging.DistributedMessageBus;
import io.atomix.messaging.DistributedTaskQueue;
import io.atomix.messaging.DistributedTopic;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceType;
import io.atomix.variables.DistributedLong;
import io.atomix.variables.DistributedValue;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Base type for creating and managing distributed {@link Resource resources} in a Atomix cluster.
 * <p>
 * Resources are user provided stateful objects backed by a distributed state machine. This class facilitates the
 * creation and management of {@link Resource} objects via a filesystem like interface. There is a
 * one-to-one relationship between keys and resources, so each key can be associated with one and only one resource.
 * <p>
 * To create a resource, pass the resource {@link java.lang.Class} to the {@link Atomix#create(String, ResourceType)} method.
 * When a resource is created, the {@link io.atomix.copycat.server.StateMachine} associated with the resource will be created on each Raft server
 * and future operations submitted for that resource will be applied to the state machine. Internally, resource state
 * machines are multiplexed across a shared Raft log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Atomix implements ResourceManager<Atomix> {
  final ResourceClient client;

  protected Atomix(ResourceClient client) {
    this.client = Assert.notNull(client, "client");
  }

  /**
   * Gets or creates a distributed map.
   *
   * @param key The resource key.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the map has been created.
   */
  public <K, V> CompletableFuture<DistributedMap<K, V>> getMap(String key) {
    return get(key, DistributedMap.class);
  }

  /**
   * Gets or creates a distributed multi map.
   *
   * @param key The resource key.
   * @param <K> The multi map key type.
   * @param <V> The multi map value type.
   * @return A completable future to be completed once the multi map has been created.
   */
  public <K, V> CompletableFuture<DistributedMultiMap<K, V>> getMultiMap(String key) {
    return get(key, DistributedMultiMap.class);
  }

  /**
   * Gets or creates a distributed set.
   *
   * @param key The resource key.
   * @param <T> The value type.
   * @return A completable future to be completed once the set has been created.
   */
  public <T> CompletableFuture<DistributedSet<T>> getSet(String key) {
    return get(key, DistributedSet.class);
  }

  /**
   * Gets or creates a distributed queue.
   *
   * @param key The resource key.
   * @param <T> The value type.
   * @return A completable future to be completed once the queue has been created.
   */
  public <T> CompletableFuture<DistributedQueue<T>> getQueue(String key) {
    return get(key, DistributedQueue.class);
  }

  /**
   * Gets or creates a distributed value.
   *
   * @param key The resource key.
   * @param <T> The value type.
   * @return A completable future to be completed once the value has been created.
   */
  public <T> CompletableFuture<DistributedValue<T>> getValue(String key) {
    return get(key, DistributedValue.class);
  }

  /**
   * Gets or creates a distributed long.
   *
   * @param key The resource key.
   * @return A completable future to be completed once the long has been created.
   */
  public CompletableFuture<DistributedLong> getLong(String key) {
    return get(key, DistributedLong.class);
  }

  /**
   * Gets or creates a distributed lock.
   *
   * @param key The resource key.
   * @return A completable future to be completed once the lock has been created.
   */
  public CompletableFuture<DistributedLock> getLock(String key) {
    return get(key, DistributedLock.class);
  }

  /**
   * Gets or creates a distributed membership group.
   *
   * @param key The resource key.
   * @return A completable future to be completed once the membership group has been created.
   */
  public CompletableFuture<DistributedMembershipGroup> getMembershipGroup(String key) {
    return get(key, DistributedMembershipGroup.class);
  }

  /**
   * Gets or creates a distributed topic.
   *
   * @param key The resource key.
   * @param <T> The topic message type.
   * @return A completable future to be completed once the topic has been created.
   */
  public <T> CompletableFuture<DistributedTopic<T>> getTopic(String key) {
    return get(key, DistributedTopic.class);
  }

  /**
   * Gets or creates a distributed queue.
   *
   * @param key The resource key.
   * @param <T> The queue message type.
   * @return A completable future to be completed once the queue has been created.
   */
  public <T> CompletableFuture<DistributedTaskQueue<T>> getTaskQueue(String key) {
    return get(key, DistributedTaskQueue.class);
  }

  /**
   * Gets or creates a distributed message bus.
   *
   * @param key The resource key.
   * @param address The local message bus address.
   * @return A completable future to be completed once the message bus has been created.
   */
  public CompletableFuture<DistributedMessageBus> getMessageBus(String key, Address address) {
    return get(key, DistributedMessageBus.class, DistributedMessageBus.options().withAddress(address).build());
  }

  @Override
  public ThreadContext context() {
    return client.context();
  }

  @Override
  public ResourceType type(Class<? extends Resource<?, ?>> type) {
    return client.type(type);
  }

  @Override
  public CompletableFuture<Boolean> exists(String key) {
    return client.exists(key);
  }

  @Override
  public CompletableFuture<Set<String>> keys() {
    return client.keys();
  }

  @Override
  public <T extends Resource> CompletableFuture<Set<String>> keys(Class<? super T> type) {
    return client.keys(type);
  }

  @Override
  public CompletableFuture<Set<String>> keys(ResourceType type) {
    return client.keys(type);
  }

  @Override
  public <T extends Resource> CompletableFuture<T> get(String key, Class<? super T> type) {
    return client.get(key, type);
  }

  @Override
  public <T extends Resource<T, U>, U extends Resource.Options> CompletableFuture<T> get(String key, Class<? super T> type, U options) {
    return client.get(key, type, options);
  }

  @Override
  public <T extends Resource> CompletableFuture<T> get(String key, ResourceType type) {
    return client.get(key, type);
  }

  @Override
  public <T extends Resource<T, U>, U extends Resource.Options> CompletableFuture<T> get(String key, ResourceType type, U options) {
    return client.get(key, type, options);
  }

  @Override
  public CompletableFuture<Atomix> open() {
    return client.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return client.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.close();
  }

  @Override
  public boolean isClosed() {
    return client.isClosed();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
