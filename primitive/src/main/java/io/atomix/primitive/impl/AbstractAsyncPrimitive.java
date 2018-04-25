/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitive.impl;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.Proxy;
import io.atomix.storage.buffer.HeapBytes;
import io.atomix.utils.concurrent.Futures;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract base class for primitives that interact with Raft replicated state machines via proxy.
 */
public abstract class AbstractAsyncPrimitive<A extends AsyncPrimitive> implements AsyncPrimitive {
  private final Function<PartitionProxy.State, Status> mapper = state -> {
    switch (state) {
      case CONNECTED:
        return Status.ACTIVE;
      case SUSPENDED:
        return Status.SUSPENDED;
      case CLOSED:
        return Status.INACTIVE;
      default:
        throw new IllegalStateException("Unknown state " + state);
    }
  };

  private final PrimitiveProxy proxy;
  private final PrimitiveRegistry registry;
  private final Set<Consumer<Status>> statusChangeListeners = Sets.newCopyOnWriteArraySet();
  private final Map<EventType, Map<PartitionId, Map<Object, Consumer>>> eventListeners = Maps.newIdentityHashMap();
  private final Map<BiConsumer<PartitionId, Proxy.State>, Map<PartitionId, Consumer<Proxy.State>>> stateChangeListeners =
      Maps.newIdentityHashMap();

  public AbstractAsyncPrimitive(PrimitiveProxy proxy, PrimitiveRegistry registry) {
    this.proxy = checkNotNull(proxy, "proxy cannot be null");
    this.registry = checkNotNull(registry, "registry cannot be null");
    proxy.addStateChangeListener(this::onStateChange);
  }

  @Override
  public String name() {
    return proxy.name();
  }

  @Override
  public PrimitiveType primitiveType() {
    return proxy.type();
  }

  /**
   * Returns the primitive partition key.
   *
   * @return the primitive partition key
   */
  public String getPartitionKey() {
    return name();
  }

  /**
   * Returns the underlying proxy.
   *
   * @return the underlying proxy
   */
  protected PrimitiveProxy getProxy() {
    return proxy;
  }

  /**
   * Returns the collection of all partitions.
   *
   * @return the collection of all partitions
   */
  protected Collection<PartitionProxy> getPartitions() {
    return getProxy().getPartitions();
  }

  /**
   * Returns a partition by ID.
   *
   * @param partitionId the partition identifier
   * @return the partition proxy
   */
  protected PartitionProxy getPartition(PartitionId partitionId) {
    return getProxy().getPartition(partitionId);
  }

  /**
   * Returns the partition for the given key.
   *
   * @param key the key for which to return the partition
   * @return the partition proxy for the given key
   */
  protected PartitionProxy getPartition(String key) {
    return getProxy().getPartition(key);
  }

  /**
   * Submits an empty operation to all partitions, awaiting a void result.
   *
   * @param operationId the operation identifier
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it. The future will always be completed on the
   * @throws NullPointerException if {@code operation} is null
   */
  protected CompletableFuture<Void> invokes(OperationId operationId) {
    return executes(operationId).thenApply(r -> null);
  }

  /**
   * Submits an empty operation to all partitions.
   *
   * @param operationId the operation identifier
   * @param decoder     the operation result decoder
   * @param <R>         the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <R> CompletableFuture<Stream<R>> invokes(OperationId operationId, Function<byte[], R> decoder) {
    return executes(operationId)
        .thenApply(results -> results.map(decoder::apply));
  }

  /**
   * Submits an operation to all partitions.
   *
   * @param operationId the operation identifier
   * @param encoder     the operation encoder
   * @param <T>         the operation type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <T> CompletableFuture<Void> invokes(OperationId operationId, Function<T, byte[]> encoder, T operation) {
    return executes(operationId, encoder.apply(operation))
        .thenApply(r -> null);
  }

  /**
   * Submits an operation to all partitions.
   *
   * @param operationId the operation identifier
   * @param encoder     the operation encoder
   * @param operation   the operation to submit
   * @param decoder     the operation result decoder
   * @param <T>         the operation type
   * @param <R>         the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <T, R> CompletableFuture<Stream<R>> invokes(OperationId operationId, Function<T, byte[]> encoder, T operation, Function<byte[], R> decoder) {
    return executes(operationId, encoder.apply(operation))
        .thenApply(results -> results.map(decoder::apply));
  }

  /**
   * Executes an operation on all partitions.
   *
   * @param operationId the operation identifier
   * @return a completable future to be completed with the operation result
   * @throws NullPointerException if {@code command} is null
   */
  private CompletableFuture<Stream<byte[]>> executes(OperationId operationId) {
    return executes(new PrimitiveOperation(OperationId.simplify(operationId), HeapBytes.EMPTY));
  }

  /**
   * Executes an operation on all partitions.
   *
   * @param operationId the operation identifier
   * @param operation   the operation to execute
   * @return a completable future to be completed with the operation result
   * @throws NullPointerException if {@code command} is null
   */
  private CompletableFuture<Stream<byte[]>> executes(OperationId operationId, byte[] operation) {
    return executes(new PrimitiveOperation(OperationId.simplify(operationId), operation));
  }

  /**
   * Executes an operation on all partitions.
   *
   * @param operation the operation to execute
   * @return a future to be completed with the operation result
   * @throws NullPointerException if {@code operation} is null
   */
  private CompletableFuture<Stream<byte[]>> executes(PrimitiveOperation operation) {
    return Futures.allOf(getPartitions().stream().map(partition -> partition.execute(operation)));
  }

  /**
   * Adds an event listener to all partitions.
   *
   * @param eventType the event type identifier.
   * @param decoder   the event decoder.
   * @param listener  the event listener.
   * @param <T>       the event value type.
   */
  protected <T> void addEventListeners(EventType eventType, Function<byte[], T> decoder, BiConsumer<PartitionId, T> listener) {
    getPartitions().forEach(partition -> {
      Consumer<T> partitionListener = event -> listener.accept(partition.partitionId(), event);
      eventListeners.computeIfAbsent(eventType, t -> Maps.newHashMap())
          .computeIfAbsent(partition.partitionId(), p -> Maps.newIdentityHashMap())
          .put(listener, partitionListener);
      partition.addEventListener(eventType, decoder, partitionListener);
    });
  }

  /**
   * Adds an empty event listener to all partitions.
   *
   * @param eventType the event type
   * @param listener  the event listener to add
   */
  protected void addEventListeners(EventType eventType, Consumer<PartitionId> listener) {
    getPartitions().forEach(partition -> {
      Consumer<byte[]> partitionListener = event -> listener.accept(partition.partitionId());
      eventListeners.computeIfAbsent(eventType, t -> Maps.newHashMap())
          .computeIfAbsent(partition.partitionId(), p -> Maps.newIdentityHashMap())
          .put(listener, partitionListener);
      partition.addEventListener(eventType, partitionListener);
    });
  }

  /**
   * Adds an event listener to all partitions.
   *
   * @param eventType the event type identifier
   * @param listener  the event listener to add
   */
  protected void addEventListeners(EventType eventType, BiConsumer<PartitionId, byte[]> listener) {
    getPartitions().forEach(partition -> {
      Consumer<byte[]> partitionListener = event -> listener.accept(partition.partitionId(), event);
      eventListeners.computeIfAbsent(eventType, t -> Maps.newHashMap())
          .computeIfAbsent(partition.partitionId(), p -> Maps.newIdentityHashMap())
          .put(listener, partitionListener);
      partition.addEventListener(eventType, partitionListener);
    });
  }

  /**
   * Removes an empty event listener from all partitions.
   *
   * @param eventType the event type
   * @param listener  the event listener to add
   */
  protected void removeEventListeners(EventType eventType, Consumer<PartitionId> listener) {
    Map<PartitionId, Map<Object, Consumer>> eventTypeListeners = eventListeners.get(eventType);
    if (eventTypeListeners != null) {
      getPartitions().forEach(partition -> {
        Map<Object, Consumer> partitionListeners = eventTypeListeners.get(partition.partitionId());
        if (partitionListeners != null) {
          Consumer partitionListener = partitionListeners.remove(listener);
          if (partitionListener != null) {
            partition.removeEventListener(eventType, partitionListener);
          }
          if (partitionListeners.isEmpty()) {
            eventTypeListeners.remove(partition.partitionId());
          }
        }
        if (eventTypeListeners.isEmpty()) {
          eventListeners.remove(eventType);
        }
      });
    }
  }

  /**
   * Removes an event listener from all partitions.
   *
   * @param eventType the event type identifier
   * @param listener  the event listener to remove
   */
  protected void removeEventListeners(EventType eventType, BiConsumer listener) {
    Map<PartitionId, Map<Object, Consumer>> eventTypeListeners = eventListeners.get(eventType);
    if (eventTypeListeners != null) {
      getPartitions().forEach(partition -> {
        Map<Object, Consumer> partitionListeners = eventTypeListeners.get(partition.partitionId());
        if (partitionListeners != null) {
          Consumer partitionListener = partitionListeners.remove(listener);
          if (partitionListener != null) {
            partition.removeEventListener(eventType, partitionListener);
          }
          if (partitionListeners.isEmpty()) {
            eventTypeListeners.remove(partition.partitionId());
          }
        }
        if (eventTypeListeners.isEmpty()) {
          eventListeners.remove(eventType);
        }
      });
    }
  }

  /**
   * Submits an empty operation to the given partition, awaiting a void result.
   *
   * @param partitionId the partition in which to execute the operation
   * @param operationId the operation identifier
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it. The future will always be completed on the
   * @throws NullPointerException if {@code operation} is null
   */
  protected CompletableFuture<Void> invoke(PartitionId partitionId, OperationId operationId) {
    return execute(partitionId, operationId).thenApply(r -> null);
  }

  /**
   * Submits an empty operation to the given partition.
   *
   * @param partitionId the partition in which to execute the operation
   * @param operationId the operation identifier
   * @param decoder     the operation result decoder
   * @param <R>         the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <R> CompletableFuture<R> invoke(PartitionId partitionId, OperationId operationId, Function<byte[], R> decoder) {
    return execute(partitionId, operationId).thenApply(decoder);
  }

  /**
   * Submits an operation to the given partition.
   *
   * @param partitionId the partition in which to execute the operation
   * @param operationId the operation identifier
   * @param encoder     the operation encoder
   * @param <T>         the operation type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <T> CompletableFuture<Void> invoke(PartitionId partitionId, OperationId operationId, Function<T, byte[]> encoder, T operation) {
    return execute(partitionId, operationId, encoder.apply(operation)).thenApply(r -> null);
  }

  /**
   * Submits an operation to the given partition.
   *
   * @param partitionId the partition in which to execute the operation
   * @param operationId the operation identifier
   * @param encoder     the operation encoder
   * @param operation   the operation to submit
   * @param decoder     the operation result decoder
   * @param <T>         the operation type
   * @param <R>         the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <T, R> CompletableFuture<R> invoke(PartitionId partitionId, OperationId operationId, Function<T, byte[]> encoder, T operation, Function<byte[], R> decoder) {
    return execute(partitionId, operationId, encoder.apply(operation)).thenApply(decoder);
  }

  /**
   * Executes an operation to the given partition.
   *
   * @param partitionId the partition in which to execute the operation
   * @param operationId the operation identifier
   * @return a completable future to be completed with the operation result
   * @throws NullPointerException if {@code command} is null
   */
  private CompletableFuture<byte[]> execute(PartitionId partitionId, OperationId operationId) {
    return execute(partitionId, new PrimitiveOperation(OperationId.simplify(operationId), HeapBytes.EMPTY));
  }

  /**
   * Executes an operation to the given partition.
   *
   * @param partitionId the partition in which to execute the operation
   * @param operationId the operation identifier
   * @param operation   the operation to execute
   * @return a completable future to be completed with the operation result
   * @throws NullPointerException if {@code command} is null
   */
  private CompletableFuture<byte[]> execute(PartitionId partitionId, OperationId operationId, byte[] operation) {
    return execute(partitionId, new PrimitiveOperation(OperationId.simplify(operationId), operation));
  }

  /**
   * Executes an operation to the cluster.
   *
   * @param partitionId the partition in which to execute the operation
   * @param operation   the operation to execute
   * @return a future to be completed with the operation result
   * @throws NullPointerException if {@code operation} is null
   */
  private CompletableFuture<byte[]> execute(PartitionId partitionId, PrimitiveOperation operation) {
    return getPartition(partitionId).execute(operation);
  }

  /**
   * Adds an event listener.
   *
   * @param partitionId the partition to which to add the listener
   * @param eventType   the event type identifier.
   * @param decoder     the event decoder.
   * @param listener    the event listener.
   * @param <T>         the event value type.
   */
  protected <T> void addEventListener(PartitionId partitionId, EventType eventType, Function<byte[], T> decoder, Consumer<T> listener) {
    getPartition(partitionId).addEventListener(eventType, decoder, listener);
  }

  /**
   * Adds an empty session event listener.
   *
   * @param partitionId the partition to which to add the listener
   * @param eventType   the event type
   * @param listener    the event listener to add
   */
  protected void addEventListener(PartitionId partitionId, EventType eventType, Runnable listener) {
    getPartition(partitionId).addEventListener(eventType, listener);
  }

  /**
   * Adds a session event listener.
   *
   * @param partitionId the partition to which to add the listener
   * @param eventType   the event type identifier
   * @param listener    the event listener to add
   */
  protected void addEventListener(PartitionId partitionId, EventType eventType, Consumer<byte[]> listener) {
    getPartition(partitionId).addEventListener(eventType, listener);
  }

  /**
   * Removes an empty session event listener.
   *
   * @param partitionId the partition from which to remove the listener
   * @param eventType   the event type
   * @param listener    the event listener to add
   */
  protected void removeEventListener(PartitionId partitionId, EventType eventType, Runnable listener) {
    getPartition(partitionId).addEventListener(eventType, listener);
  }

  /**
   * Removes a session event listener.
   *
   * @param partitionId the partition from which to remove the listener
   * @param eventType   the event type identifier
   * @param listener    the event listener to remove
   */
  protected void removeEventListener(PartitionId partitionId, EventType eventType, Consumer listener) {
    getPartition(partitionId).addEventListener(eventType, listener);
  }

  /**
   * Submits an empty operation to the owning partition for the given key, awaiting a void result.
   *
   * @param key         the key for which to submit the operation
   * @param operationId the operation identifier
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it. The future will always be completed on the
   * @throws NullPointerException if {@code operation} is null
   */
  protected CompletableFuture<Void> invoke(String key, OperationId operationId) {
    return getPartition(key).execute(operationId).thenApply(r -> null);
  }

  /**
   * Submits an empty operation to the owning partition for the given key.
   *
   * @param key         the key for which to submit the operation
   * @param operationId the operation identifier
   * @param decoder     the operation result decoder
   * @param <R>         the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <R> CompletableFuture<R> invoke(String key, OperationId operationId, Function<byte[], R> decoder) {
    return getPartition(key).execute(operationId).thenApply(decoder);
  }

  /**
   * Submits an operation to the owning partition for the given key.
   *
   * @param key         the key for which to submit the operation
   * @param operationId the operation identifier
   * @param encoder     the operation encoder
   * @param <T>         the operation type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <T> CompletableFuture<Void> invoke(String key, OperationId operationId, Function<T, byte[]> encoder, T operation) {
    return getPartition(key).execute(operationId, encoder.apply(operation)).thenApply(r -> null);
  }

  /**
   * Submits an operation to the owning partition for the given key.
   *
   * @param key         the key for which to submit the operation
   * @param operationId the operation identifier
   * @param encoder     the operation encoder
   * @param operation   the operation to submit
   * @param decoder     the operation result decoder
   * @param <T>         the operation type
   * @param <R>         the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <T, R> CompletableFuture<R> invoke(String key, OperationId operationId, Function<T, byte[]> encoder, T operation, Function<byte[], R> decoder) {
    return getPartition(key).execute(operationId, encoder.apply(operation)).thenApply(decoder);
  }

  /**
   * Executes an operation to the owning partition for the given key.
   *
   * @param key         the key for which to submit the operation
   * @param operationId the operation identifier
   * @return a completable future to be completed with the operation result
   * @throws NullPointerException if {@code command} is null
   */
  private CompletableFuture<byte[]> execute(String key, OperationId operationId) {
    return getPartition(key).execute(new PrimitiveOperation(OperationId.simplify(operationId), HeapBytes.EMPTY));
  }

  /**
   * Executes an operation to the owning partition for the given key.
   *
   * @param key         the key for which to submit the operation
   * @param operationId the operation identifier
   * @param operation   the operation to execute
   * @return a completable future to be completed with the operation result
   * @throws NullPointerException if {@code command} is null
   */
  private CompletableFuture<byte[]> execute(String key, OperationId operationId, byte[] operation) {
    return getPartition(key).execute(new PrimitiveOperation(OperationId.simplify(operationId), operation));
  }

  /**
   * Executes an operation to the owning partition for the given key.
   *
   * @param key       the key for which to submit the operation
   * @param operation the operation to execute
   * @return a future to be completed with the operation result
   * @throws NullPointerException if {@code operation} is null
   */
  private CompletableFuture<byte[]> execute(String key, PrimitiveOperation operation) {
    return getPartition(key).execute(operation);
  }

  /**
   * Adds an event listener to the owning partition for the given key.
   *
   * @param key       the key for which to add the listener
   * @param eventType the event type identifier.
   * @param decoder   the event decoder.
   * @param listener  the event listener.
   * @param <T>       the event value type.
   */
  protected <T> void addEventListener(String key, EventType eventType, Function<byte[], T> decoder, Consumer<T> listener) {
    getPartition(key).addEventListener(eventType, decoder, listener);
  }

  /**
   * Adds an empty event listener to the owning partition for the given key.
   *
   * @param key       the key for which to add the listener
   * @param eventType the event type
   * @param listener  the event listener to add
   */
  protected void addEventListener(String key, EventType eventType, Runnable listener) {
    getPartition(key).addEventListener(eventType, listener);
  }

  /**
   * Adds a session event listener to the owning partition for the given key.
   *
   * @param key       the key for which to add the listener
   * @param eventType the event type identifier
   * @param listener  the event listener to add
   */
  protected void addEventListener(String key, EventType eventType, Consumer<byte[]> listener) {
    getPartition(key).addEventListener(eventType, listener);
  }

  /**
   * Removes an empty event listener to the owning partition for the given key.
   *
   * @param key       the key for which to remove the listener
   * @param eventType the event type
   * @param listener  the event listener to add
   */
  protected void removeEventListener(String key, EventType eventType, Runnable listener) {
    getPartition(key).addEventListener(eventType, listener);
  }

  /**
   * Removes an event listener to the owning partition for the given key.
   *
   * @param key       the key for which to remove the listener
   * @param eventType the event type identifier
   * @param listener  the event listener to remove
   */
  protected void removeEventListener(String key, EventType eventType, Consumer listener) {
    getPartition(key).addEventListener(eventType, listener);
  }

  /**
   * Adds a state change listener to the given partition.
   *
   * @param partitionId the partition to which to add the listener
   * @param listener    the partition to which to add the listener
   */
  protected void addStateChangeListener(PartitionId partitionId, Consumer<Proxy.State> listener) {
    getPartition(partitionId).addStateChangeListener(listener);
  }

  /**
   * Adds a state change listener to the given partition.
   *
   * @param partitionId the partition to which to add the listener
   * @param listener    the partition to which to add the listener
   */
  protected void removeStateChangeListener(PartitionId partitionId, Consumer<Proxy.State> listener) {
    getPartition(partitionId).removeStateChangeListener(listener);
  }

  /**
   * Adds a state change listener to the given partition.
   *
   * @param key      the key for the partition to which to add the listener
   * @param listener the partition to which to add the listener
   */
  protected void addStateChangeListener(String key, Consumer<Proxy.State> listener) {
    getPartition(key).addStateChangeListener(listener);
  }

  /**
   * Adds a state change listener to the given partition.
   *
   * @param key      the key for the partition to which to add the listener
   * @param listener the partition to which to add the listener
   */
  protected void removeStateChangeListener(String key, Consumer<Proxy.State> listener) {
    getPartition(key).removeStateChangeListener(listener);
  }

  /**
   * Adds a state change listener to all partitions.
   *
   * @param listener the listener to add
   */
  protected synchronized void addStateChangeListeners(BiConsumer<PartitionId, Proxy.State> listener) {
    getPartitions().forEach(partition -> {
      Consumer<Proxy.State> partitionListener = state -> listener.accept(partition.partitionId(), state);
      partition.addStateChangeListener(partitionListener);
      stateChangeListeners.computeIfAbsent(listener, l -> Maps.newHashMap()).put(partition.partitionId(), partitionListener);
    });
  }

  /**
   * Removes a state change listener from all partitions.
   *
   * @param listener the listener to remove
   */
  protected synchronized void removeStateChangeListeners(BiConsumer<PartitionId, Proxy.State> listener) {
    Map<PartitionId, Consumer<Proxy.State>> partitionListeners = stateChangeListeners.remove(listener);
    if (partitionListeners != null) {
      getPartitions().forEach(partition -> {
        Consumer<Proxy.State> partitionListener = partitionListeners.get(partition.partitionId());
        if (partitionListener != null) {
          partition.removeStateChangeListener(partitionListener);
        }
      });
    }
  }

  /**
   * Handles a Raft session state change.
   *
   * @param state the updated Raft session state
   */
  private void onStateChange(Proxy.State state) {
    statusChangeListeners.forEach(listener -> listener.accept(mapper.apply(state)));
  }

  @Override
  public void addStatusChangeListener(Consumer<Status> listener) {
    statusChangeListeners.add(listener);
  }

  @Override
  public void removeStatusChangeListener(Consumer<Status> listener) {
    statusChangeListeners.remove(listener);
  }

  @Override
  public Collection<Consumer<Status>> statusChangeListeners() {
    return ImmutableSet.copyOf(statusChangeListeners);
  }

  /**
   * Connects the primitive.
   *
   * @return a future to be completed once the primitive has been connected
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<A> connect() {
    return registry.createPrimitive(name(), primitiveType())
        .thenApply(v -> (A) this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return proxy.close();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("proxy", proxy)
        .toString();
  }
}