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
package io.atomix.protocols.log.impl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.log.LogClient;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.LogProtocol;
import io.atomix.utils.concurrent.Futures;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Partitioned log client.
 */
public class DistributedLogClient implements LogClient {
  private final LogProtocol protocol;
  private final List<PartitionId> partitionIds = new CopyOnWriteArrayList<>();
  private final Map<PartitionId, LogSession> partitions = Maps.newConcurrentMap();
  private final List<LogSession> sortedPartitions = new CopyOnWriteArrayList<>();
  private final Partitioner<String> partitioner;
  private final Set<Consumer<PrimitiveState>> stateChangeListeners = Sets.newCopyOnWriteArraySet();
  private final Map<PartitionId, PrimitiveState> states = Maps.newHashMap();
  private volatile PrimitiveState state = PrimitiveState.CLOSED;

  public DistributedLogClient(
      LogProtocol protocol,
      Collection<LogSession> partitions,
      Partitioner<String> partitioner) {
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.partitioner = checkNotNull(partitioner, "partitioner cannot be null");
    partitions.forEach(partition -> {
      this.partitionIds.add(partition.partitionId());
      this.partitions.put(partition.partitionId(), partition);
      this.sortedPartitions.add(partition);
      states.put(partition.partitionId(), PrimitiveState.CLOSED);
      partition.addStateChangeListener(state -> onStateChange(partition.partitionId(), state));
    });
  }

  @Override
  public LogProtocol protocol() {
    return protocol;
  }

  @Override
  public PrimitiveState state() {
    return state;
  }

  @Override
  public Collection<LogSession> getPartitions() {
    return sortedPartitions;
  }

  @Override
  public Collection<PartitionId> getPartitionIds() {
    return partitions.keySet();
  }

  @Override
  public LogSession getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  public PartitionId getPartitionId(String key) {
    return partitioner.partition(key, partitionIds);
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    stateChangeListeners.add(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    stateChangeListeners.remove(listener);
  }

  private synchronized void onStateChange(PartitionId partitionId, PrimitiveState state) {
    states.put(partitionId, state);
    switch (state) {
      case CONNECTED:
        if (!states.containsValue(PrimitiveState.SUSPENDED) && !states.containsValue(PrimitiveState.CLOSED)) {
          changeState(PrimitiveState.CONNECTED);
        }
        break;
      case SUSPENDED:
        changeState(PrimitiveState.SUSPENDED);
        break;
      case CLOSED:
        changeState(PrimitiveState.CLOSED);
        break;
    }
  }

  private synchronized void changeState(PrimitiveState state) {
    if (this.state != state) {
      this.state = state;
      stateChangeListeners.forEach(l -> l.accept(state));
    }
  }

  @Override
  public CompletableFuture<LogClient> connect() {
    return Futures.allOf(partitions.values().stream().map(LogSession::connect)).thenApply(v -> {
      changeState(PrimitiveState.CONNECTED);
      return this;
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    return Futures.allOf(partitions.values().stream().map(LogSession::connect)).thenRun(() -> {
      changeState(PrimitiveState.CLOSED);
    });
  }
}
