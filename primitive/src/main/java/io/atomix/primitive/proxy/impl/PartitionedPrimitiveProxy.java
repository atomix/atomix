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
package io.atomix.primitive.proxy.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.concurrent.Futures;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default primitive proxy.
 */
public class PartitionedPrimitiveProxy implements PrimitiveProxy {
  private final String name;
  private final PrimitiveType type;
  private final List<PartitionId> partitionIds = new CopyOnWriteArrayList<>();
  private final Map<PartitionId, PartitionProxy> partitions = Maps.newConcurrentMap();
  private final Partitioner<String> partitioner;
  private final Set<Consumer<PartitionProxy.State>> stateChangeListeners = Sets.newCopyOnWriteArraySet();
  private final Map<PartitionId, State> states = Maps.newHashMap();
  private volatile State state = State.CLOSED;

  public PartitionedPrimitiveProxy(
      String name,
      PrimitiveType type,
      Collection<PartitionProxy> partitions,
      Partitioner<String> partitioner) {
    this.name = checkNotNull(name, "name cannot be null");
    this.type = checkNotNull(type, "type cannot be null");
    this.partitioner = checkNotNull(partitioner, "partitioner cannot be null");
    partitions.forEach(partition -> {
      this.partitionIds.add(partition.partitionId());
      this.partitions.put(partition.partitionId(), new LazyPartitionProxy(partition));
      states.put(partition.partitionId(), State.CLOSED);
    });
    Collections.sort(partitionIds);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return type;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public Collection<PartitionProxy> getPartitions() {
    return partitions.values();
  }

  @Override
  public Collection<PartitionId> getPartitionIds() {
    return partitions.keySet();
  }

  @Override
  public PartitionProxy getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  public PartitionId getPartitionId(String key) {
    return partitioner.partition(key, partitionIds);
  }

  @Override
  public void addStateChangeListener(Consumer<PartitionProxy.State> listener) {
    stateChangeListeners.add(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PartitionProxy.State> listener) {
    stateChangeListeners.remove(listener);
  }

  /**
   * Handles a partition proxy state change.
   */
  private synchronized void onStateChange(PartitionId partitionId, PartitionProxy.State state) {
    states.put(partitionId, state);
    switch (state) {
      case CONNECTED:
        if (this.state != State.CONNECTED && !states.containsValue(State.SUSPENDED) && !states.containsValue(State.CLOSED)) {
          this.state = State.CONNECTED;
          stateChangeListeners.forEach(l -> l.accept(State.CONNECTED));
        }
        break;
      case SUSPENDED:
        if (this.state == State.CONNECTED) {
          this.state = State.SUSPENDED;
          stateChangeListeners.forEach(l -> l.accept(State.SUSPENDED));
        }
        break;
      case CLOSED:
        if (this.state != State.CLOSED) {
          this.state = State.CLOSED;
          stateChangeListeners.forEach(l -> l.accept(State.CLOSED));
        }
        break;
    }
  }

  @Override
  public CompletableFuture<PrimitiveProxy> connect() {
    partitions.forEach((partitionId, partition) -> {
      partition.addStateChangeListener(state -> onStateChange(partitionId, state));
    });
    return Futures.allOf(partitions.values()
        .stream()
        .map(PartitionProxy::connect)
        .collect(Collectors.toList()))
        .thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return Futures.allOf(partitions.values()
        .stream()
        .map(PartitionProxy::close)
        .collect(Collectors.toList()))
        .thenApply(v -> null);
  }
}
