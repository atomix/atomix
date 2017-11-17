/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.backup.partition.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.counter.AsyncAtomicCounter;
import io.atomix.counter.impl.AtomicCounterProxy;
import io.atomix.generator.AsyncAtomicIdGenerator;
import io.atomix.generator.impl.DelegatingIdGenerator;
import io.atomix.leadership.AsyncLeaderElector;
import io.atomix.leadership.impl.LeaderElectorProxy;
import io.atomix.leadership.impl.TranscodingAsyncLeaderElector;
import io.atomix.lock.AsyncDistributedLock;
import io.atomix.lock.impl.DistributedLockProxy;
import io.atomix.map.AsyncAtomicCounterMap;
import io.atomix.map.AsyncConsistentMap;
import io.atomix.map.AsyncConsistentTreeMap;
import io.atomix.map.impl.AtomicCounterMapProxy;
import io.atomix.map.impl.ConsistentMapProxy;
import io.atomix.map.impl.ConsistentTreeMapProxy;
import io.atomix.multimap.AsyncConsistentMultimap;
import io.atomix.multimap.impl.ConsistentSetMultimapProxy;
import io.atomix.primitive.DistributedPrimitiveCreator;
import io.atomix.primitive.DistributedPrimitives;
import io.atomix.primitive.Ordering;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypes;
import io.atomix.protocols.backup.PrimaryBackupClient;
import io.atomix.protocols.backup.ReplicaInfoProvider;
import io.atomix.queue.AsyncWorkQueue;
import io.atomix.queue.impl.TranscodingAsyncWorkQueue;
import io.atomix.queue.impl.WorkQueueProxy;
import io.atomix.serializer.Serializer;
import io.atomix.set.AsyncDistributedSet;
import io.atomix.tree.AsyncDocumentTree;
import io.atomix.tree.impl.DocumentTreeProxy;
import io.atomix.tree.impl.TranscodingAsyncDocumentTree;
import io.atomix.utils.Managed;
import io.atomix.value.AsyncAtomicValue;
import io.atomix.value.impl.AtomicValueProxy;
import io.atomix.value.impl.TranscodingAsyncAtomicValue;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Primary-backup partition client.
 */
public class PrimaryBackupPartitionClient implements DistributedPrimitiveCreator, Managed<PrimaryBackupPartitionClient> {
  private final PrimaryBackupPartition partition;
  private final ClusterService clusterService;
  private final ClusterCommunicationService clusterCommunicator;
  private final ReplicaInfoProvider replicaProvider;
  private PrimaryBackupClient client;

  public PrimaryBackupPartitionClient(
      PrimaryBackupPartition partition,
      ClusterService clusterService,
      ClusterCommunicationService clusterCommunicator,
      ReplicaInfoProvider replicaProvider) {
    this.partition = partition;
    this.clusterService = clusterService;
    this.clusterCommunicator = clusterCommunicator;
    this.replicaProvider = replicaProvider;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> AsyncConsistentMap<K, V> newAsyncConsistentMap(String name, Serializer serializer) {
    ConsistentMapProxy rawMap = new ConsistentMapProxy(client.proxyBuilder()
        .withName(name)
        .withPrimitiveType(PrimitiveTypes.CONSISTENT_MAP.name())
        .withMaxRetries(5)
        .build()
        .open()
        .join());

    if (serializer != null) {
      return DistributedPrimitives.newTranscodingMap(rawMap,
          key -> BaseEncoding.base16().encode(serializer.encode(key)),
          string -> serializer.decode(BaseEncoding.base16().decode(string)),
          value -> value == null ? null : serializer.encode(value),
          bytes -> serializer.decode(bytes));
    }
    return (AsyncConsistentMap<K, V>) rawMap;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> AsyncConsistentTreeMap<K, V> newAsyncConsistentTreeMap(String name, Serializer serializer) {
    ConsistentTreeMapProxy rawMap =
        new ConsistentTreeMapProxy(client.proxyBuilder()
            .withName(name)
            .withPrimitiveType(PrimitiveTypes.CONSISTENT_TREEMAP.name())
            .withMaxRetries(5)
            .build()
            .open()
            .join());

    if (serializer != null) {
      return DistributedPrimitives.newTranscodingTreeMap(
          rawMap,
          key -> BaseEncoding.base16().encode(serializer.encode(key)),
          string -> serializer.decode(BaseEncoding.base16().decode(string)),
          value -> value == null ? null : serializer.encode(value),
          bytes -> serializer.decode(bytes));
    }
    return (AsyncConsistentTreeMap<K, V>) rawMap;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> AsyncConsistentMultimap<K, V> newAsyncConsistentSetMultimap(String name, Serializer serializer) {
    ConsistentSetMultimapProxy rawMap =
        new ConsistentSetMultimapProxy(client.proxyBuilder()
            .withName(name)
            .withPrimitiveType(PrimitiveTypes.CONSISTENT_MULTIMAP.name())
            .withMaxRetries(5)
            .build()
            .open()
            .join());

    if (serializer != null) {
      return DistributedPrimitives.newTranscodingMultimap(
          rawMap,
          key -> BaseEncoding.base16().encode(serializer.encode(key)),
          string -> serializer.decode(BaseEncoding.base16().decode(string)),
          value -> serializer.encode(value),
          bytes -> serializer.decode(bytes));
    }
    return (AsyncConsistentMultimap<K, V>) rawMap;
  }

  @Override
  public <E> AsyncDistributedSet<E> newAsyncDistributedSet(String name, Serializer serializer) {
    return DistributedPrimitives.newSetFromMap(newAsyncConsistentMap(name, serializer));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K> AsyncAtomicCounterMap<K> newAsyncAtomicCounterMap(String name, Serializer serializer) {
    AtomicCounterMapProxy rawMap = new AtomicCounterMapProxy(client.proxyBuilder()
        .withName(name)
        .withPrimitiveType(PrimitiveTypes.COUNTER_MAP.name())
        .withMaxRetries(5)
        .build()
        .open()
        .join());

    if (serializer != null) {
      return DistributedPrimitives.newTranscodingAtomicCounterMap(
          rawMap,
          key -> BaseEncoding.base16().encode(serializer.encode(key)),
          string -> serializer.decode(BaseEncoding.base16().decode(string)));
    }
    return (AsyncAtomicCounterMap<K>) rawMap;
  }

  @Override
  public AsyncAtomicCounter newAsyncCounter(String name) {
    return new AtomicCounterProxy(client.proxyBuilder()
        .withName(name)
        .withPrimitiveType(PrimitiveTypes.COUNTER.name())
        .withMaxRetries(5)
        .build()
        .open()
        .join());
  }

  @Override
  public AsyncAtomicIdGenerator newAsyncIdGenerator(String name) {
    return new DelegatingIdGenerator(newAsyncCounter(name));
  }

  @Override
  public <V> AsyncAtomicValue<V> newAsyncAtomicValue(String name, Serializer serializer) {
    AtomicValueProxy value = new AtomicValueProxy(client.proxyBuilder()
        .withName(name)
        .withPrimitiveType(PrimitiveTypes.VALUE.name())
        .withMaxRetries(5)
        .build()
        .open()
        .join());
    return new TranscodingAsyncAtomicValue<>(value, serializer::encode, serializer::decode);
  }

  @Override
  public <E> AsyncWorkQueue<E> newAsyncWorkQueue(String name, Serializer serializer) {
    WorkQueueProxy workQueue = new WorkQueueProxy(client.proxyBuilder()
        .withName(name)
        .withPrimitiveType(PrimitiveTypes.WORK_QUEUE.name())
        .withMaxRetries(5)
        .build()
        .open()
        .join());
    return new TranscodingAsyncWorkQueue<>(workQueue, serializer::encode, serializer::decode);
  }

  @Override
  public <V> AsyncDocumentTree<V> newAsyncDocumentTree(String name, Serializer serializer, Ordering ordering) {
    DocumentTreeProxy documentTree = new DocumentTreeProxy(client.proxyBuilder()
        .withName(name)
        .withPrimitiveType(String.format("%s-%s", PrimitiveTypes.DOCUMENT_TREE.name(), ordering))
        .withMaxRetries(5)
        .build()
        .open()
        .join());
    return new TranscodingAsyncDocumentTree<>(documentTree, serializer::encode, serializer::decode);
  }

  @Override
  public <T> AsyncLeaderElector<T> newAsyncLeaderElector(String name, Serializer serializer, Duration electionTimeout) {
    LeaderElectorProxy leaderElector = new LeaderElectorProxy(client.proxyBuilder()
        .withName(name)
        .withPrimitiveType(PrimitiveTypes.LEADER_ELECTOR.name())
        .withMaxRetries(5)
        .build()
        .open()
        .join());
    return new TranscodingAsyncLeaderElector<>(leaderElector, serializer::encode, serializer::decode);
  }

  @Override
  public AsyncDistributedLock newAsyncDistributedLock(String name, Duration lockTimeout) {
    return new DistributedLockProxy(client.proxyBuilder()
        .withName(name)
        .withPrimitiveType(PrimitiveTypes.LOCK.name())
        .withMaxRetries(5)
        .build()
        .open()
        .join());
  }

  @Override
  public Set<String> getPrimitiveNames(PrimitiveType primitiveType) {
    return client.getPrimitives(primitiveType).join();
  }

  @Override
  public CompletableFuture<PrimaryBackupPartitionClient> open() {
    synchronized (PrimaryBackupPartitionClient.this) {
      client = newClient();
    }
    return CompletableFuture.completedFuture(this);
  }

  private PrimaryBackupClient newClient() {
    return PrimaryBackupClient.builder()
        .withClusterService(clusterService)
        .withCommunicationService(clusterCommunicator)
        .withReplicaProvider(replicaProvider)
        .build();
  }

  @Override
  public boolean isOpen() {
    return client != null;
  }

  @Override
  public CompletableFuture<Void> close() {
    return client != null ? client.close() : CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return client == null;
  }
}
