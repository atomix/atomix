/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.partition.impl;

import com.google.common.io.BaseEncoding;
import io.atomix.primitives.DistributedPrimitive;
import io.atomix.primitives.DistributedPrimitive.Type;
import io.atomix.primitives.DistributedPrimitiveCreator;
import io.atomix.primitives.DistributedPrimitives;
import io.atomix.primitives.Ordering;
import io.atomix.primitives.counter.AsyncAtomicCounter;
import io.atomix.primitives.counter.impl.RaftAtomicCounter;
import io.atomix.primitives.generator.AsyncAtomicIdGenerator;
import io.atomix.primitives.generator.impl.RaftIdGenerator;
import io.atomix.primitives.leadership.AsyncLeaderElector;
import io.atomix.primitives.leadership.impl.RaftLeaderElector;
import io.atomix.primitives.leadership.impl.TranscodingAsyncLeaderElector;
import io.atomix.primitives.lock.AsyncDistributedLock;
import io.atomix.primitives.lock.impl.RaftDistributedLock;
import io.atomix.primitives.map.AsyncAtomicCounterMap;
import io.atomix.primitives.map.AsyncConsistentMap;
import io.atomix.primitives.map.AsyncConsistentTreeMap;
import io.atomix.primitives.map.impl.RaftAtomicCounterMap;
import io.atomix.primitives.map.impl.RaftConsistentMap;
import io.atomix.primitives.map.impl.RaftConsistentTreeMap;
import io.atomix.primitives.multimap.AsyncConsistentMultimap;
import io.atomix.primitives.multimap.impl.RaftConsistentSetMultimap;
import io.atomix.primitives.queue.AsyncWorkQueue;
import io.atomix.primitives.queue.impl.RaftWorkQueue;
import io.atomix.primitives.queue.impl.TranscodingAsyncWorkQueue;
import io.atomix.primitives.set.AsyncDistributedSet;
import io.atomix.primitives.tree.AsyncDocumentTree;
import io.atomix.primitives.tree.impl.RaftDocumentTree;
import io.atomix.primitives.tree.impl.TranscodingAsyncDocumentTree;
import io.atomix.primitives.value.AsyncAtomicValue;
import io.atomix.primitives.value.impl.RaftAtomicValue;
import io.atomix.primitives.value.impl.TranscodingAsyncAtomicValue;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;
import io.atomix.protocols.raft.session.RaftSessionMetadata;
import io.atomix.serializer.Serializer;
import io.atomix.utils.Managed;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * StoragePartition client.
 */
public class RaftPartitionClient implements DistributedPrimitiveCreator, Managed<RaftPartitionClient> {

  private final Logger log = getLogger(getClass());

  private final RaftPartition partition;
  private final MemberId localMemberId;
  private final RaftClientProtocol protocol;
  private RaftClient client;

  public RaftPartitionClient(RaftPartition partition, MemberId localMemberId, RaftClientProtocol protocol) {
    this.partition = partition;
    this.localMemberId = localMemberId;
    this.protocol = protocol;
  }

  @Override
  public CompletableFuture<RaftPartitionClient> open() {
    synchronized (RaftPartitionClient.this) {
      client = newRaftClient(protocol);
    }
    return client.connect(partition.getMemberIds()).whenComplete((r, e) -> {
      if (e == null) {
        log.info("Successfully started client for partition {}", partition.id());
      } else {
        log.info("Failed to start client for partition {}", partition.id(), e);
      }
    }).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return client != null ? client.close() : CompletableFuture.completedFuture(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> AsyncConsistentMap<K, V> newAsyncConsistentMap(String name, Serializer serializer) {
    RaftConsistentMap rawMap = new RaftConsistentMap(client.newProxyBuilder()
        .withName(name)
        .withServiceType(DistributedPrimitive.Type.CONSISTENT_MAP.name())
        .withReadConsistency(ReadConsistency.SEQUENTIAL)
        .withCommunicationStrategy(CommunicationStrategy.ANY)
        .withTimeout(Duration.ofSeconds(30))
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
    RaftConsistentTreeMap rawMap =
        new RaftConsistentTreeMap(client.newProxyBuilder()
            .withName(name)
            .withServiceType(DistributedPrimitive.Type.CONSISTENT_TREEMAP.name())
            .withReadConsistency(ReadConsistency.SEQUENTIAL)
            .withCommunicationStrategy(CommunicationStrategy.ANY)
            .withTimeout(Duration.ofSeconds(30))
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
    RaftConsistentSetMultimap rawMap =
        new RaftConsistentSetMultimap(client.newProxyBuilder()
            .withName(name)
            .withServiceType(DistributedPrimitive.Type.CONSISTENT_MULTIMAP.name())
            .withReadConsistency(ReadConsistency.SEQUENTIAL)
            .withCommunicationStrategy(CommunicationStrategy.ANY)
            .withTimeout(Duration.ofSeconds(30))
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
    RaftAtomicCounterMap rawMap = new RaftAtomicCounterMap(client.newProxyBuilder()
        .withName(name)
        .withServiceType(DistributedPrimitive.Type.COUNTER_MAP.name())
        .withReadConsistency(ReadConsistency.LINEARIZABLE_LEASE)
        .withCommunicationStrategy(CommunicationStrategy.LEADER)
        .withTimeout(Duration.ofSeconds(30))
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
    return new RaftAtomicCounter(client.newProxyBuilder()
        .withName(name)
        .withServiceType(DistributedPrimitive.Type.COUNTER.name())
        .withReadConsistency(ReadConsistency.LINEARIZABLE_LEASE)
        .withCommunicationStrategy(CommunicationStrategy.LEADER)
        .withTimeout(Duration.ofSeconds(30))
        .withMaxRetries(5)
        .build()
        .open()
        .join());
  }

  @Override
  public AsyncAtomicIdGenerator newAsyncIdGenerator(String name) {
    return new RaftIdGenerator(newAsyncCounter(name));
  }

  @Override
  public <V> AsyncAtomicValue<V> newAsyncAtomicValue(String name, Serializer serializer) {
    RaftAtomicValue value = new RaftAtomicValue(client.newProxyBuilder()
        .withName(name)
        .withServiceType(DistributedPrimitive.Type.VALUE.name())
        .withReadConsistency(ReadConsistency.LINEARIZABLE_LEASE)
        .withCommunicationStrategy(CommunicationStrategy.ANY)
        .withTimeout(Duration.ofSeconds(30))
        .withMaxRetries(5)
        .build()
        .open()
        .join());
    return new TranscodingAsyncAtomicValue<>(value, serializer::encode, serializer::decode);
  }

  @Override
  public <E> AsyncWorkQueue<E> newAsyncWorkQueue(String name, Serializer serializer) {
    RaftWorkQueue workQueue = new RaftWorkQueue(client.newProxyBuilder()
        .withName(name)
        .withServiceType(DistributedPrimitive.Type.WORK_QUEUE.name())
        .withReadConsistency(ReadConsistency.LINEARIZABLE_LEASE)
        .withCommunicationStrategy(CommunicationStrategy.LEADER)
        .withTimeout(Duration.ofSeconds(5))
        .withMaxRetries(5)
        .build()
        .open()
        .join());
    return new TranscodingAsyncWorkQueue<>(workQueue, serializer::encode, serializer::decode);
  }

  @Override
  public <V> AsyncDocumentTree<V> newAsyncDocumentTree(String name, Serializer serializer, Ordering ordering) {
    RaftDocumentTree documentTree = new RaftDocumentTree(client.newProxyBuilder()
        .withName(name)
        .withServiceType(String.format("%s-%s", DistributedPrimitive.Type.DOCUMENT_TREE.name(), ordering))
        .withReadConsistency(ReadConsistency.SEQUENTIAL)
        .withCommunicationStrategy(CommunicationStrategy.ANY)
        .withTimeout(Duration.ofSeconds(30))
        .withMaxRetries(5)
        .build()
        .open()
        .join());
    return new TranscodingAsyncDocumentTree<>(documentTree, serializer::encode, serializer::decode);
  }

  @Override
  public <T> AsyncLeaderElector<T> newAsyncLeaderElector(String name, Serializer serializer, Duration electionTimeout) {
    RaftLeaderElector leaderElector = new RaftLeaderElector(client.newProxyBuilder()
        .withName(name)
        .withServiceType(DistributedPrimitive.Type.LEADER_ELECTOR.name())
        .withReadConsistency(ReadConsistency.LINEARIZABLE)
        .withCommunicationStrategy(CommunicationStrategy.LEADER)
        .withMinTimeout(electionTimeout)
        .withMaxTimeout(Duration.ofSeconds(5))
        .withMaxRetries(5)
        .build()
        .open()
        .join());
    return new TranscodingAsyncLeaderElector<>(leaderElector, serializer::encode, serializer::decode);
  }

  @Override
  public AsyncDistributedLock newAsyncDistributedLock(String name, Duration lockTimeout) {
    return new RaftDistributedLock(client.newProxyBuilder()
        .withName(name)
        .withServiceType(DistributedPrimitive.Type.LOCK.name())
        .withReadConsistency(ReadConsistency.LINEARIZABLE)
        .withCommunicationStrategy(CommunicationStrategy.LEADER)
        .withMinTimeout(lockTimeout)
        .withMaxTimeout(Duration.ofSeconds(5))
        .withMaxRetries(5)
        .build()
        .open()
        .join());
  }

  @Override
  public Set<String> getPrimitiveNames(Type primitiveType) {
    return client.metadata().getSessions(primitiveType.name())
        .join()
        .stream()
        .map(RaftSessionMetadata::serviceName)
        .collect(Collectors.toSet());
  }

  @Override
  public boolean isOpen() {
    return client != null;
  }

  @Override
  public boolean isClosed() {
    return client == null;
  }

  private RaftClient newRaftClient(RaftClientProtocol protocol) {
    return RaftClient.builder()
        .withClientId(partition.name())
        .withMemberId(localMemberId)
        .withProtocol(protocol)
        .build();
  }
}
