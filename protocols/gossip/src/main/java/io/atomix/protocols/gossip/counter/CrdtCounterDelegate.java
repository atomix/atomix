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
package io.atomix.protocols.gossip.counter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AtomicLongMap;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.counter.CounterDelegate;
import io.atomix.protocols.gossip.CrdtProtocolConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * CRDT based counter implementation.
 */
public class CrdtCounterDelegate implements CounterDelegate {
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .register(MemberId.class)
      .build());

  private final MemberId localMemberId;
  private final ClusterCommunicationService clusterCommunicator;
  private final ScheduledExecutorService executorService;
  private final String subject;
  private volatile ScheduledFuture<?> broadcastFuture;
  private final AtomicLongMap<MemberId> increments = AtomicLongMap.create();
  private final AtomicLongMap<MemberId> decrements = AtomicLongMap.create();

  public CrdtCounterDelegate(String name, CrdtProtocolConfig config, PrimitiveManagementService managementService) {
    this.localMemberId = managementService.getMembershipService().getLocalMember().id();
    this.clusterCommunicator = managementService.getCommunicationService();
    this.executorService = managementService.getExecutorService();
    this.subject = String.format("atomix-crdt-counter-%s", name);
    clusterCommunicator.subscribe(subject, SERIALIZER::decode, this::updateCounters, executorService);
    broadcastFuture = executorService.scheduleAtFixedRate(
        this::broadcastCounters, config.getGossipInterval().toMillis(), config.getGossipInterval().toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public long get() {
    return increments.sum() - decrements.sum();
  }

  @Override
  public long incrementAndGet() {
    return getIncrement(increments.incrementAndGet(localMemberId));
  }

  @Override
  public long decrementAndGet() {
    return getDecrement(decrements.incrementAndGet(localMemberId));
  }

  @Override
  public long getAndIncrement() {
    return getIncrement(increments.getAndIncrement(localMemberId));
  }

  @Override
  public long getAndDecrement() {
    return getDecrement(decrements.getAndIncrement(localMemberId));
  }

  @Override
  public long getAndAdd(long delta) {
    return getIncrement(increments.getAndAdd(localMemberId, delta));
  }

  @Override
  public long addAndGet(long delta) {
    return getIncrement(increments.addAndGet(localMemberId, delta));
  }

  private long getIncrement(long local) {
    return (increments.asMap().entrySet()
        .stream()
        .filter(e -> !e.getKey().equals(localMemberId))
        .mapToLong(e -> e.getValue())
        .sum() + local) - decrements.sum();
  }

  private long getDecrement(long local) {
    return increments.sum() - (decrements.asMap().entrySet()
        .stream()
        .filter(e -> !e.getKey().equals(localMemberId))
        .mapToLong(e -> e.getValue())
        .sum() + local);
  }

  private void updateCounters(List<Map<MemberId, Long>> counters) {
    Map<MemberId, Long> increments = counters.get(0);
    for (Map.Entry<MemberId, Long> entry : increments.entrySet()) {
      this.increments.accumulateAndGet(entry.getKey(), entry.getValue(), Math::max);
    }

    Map<MemberId, Long> decrements = counters.get(1);
    for (Map.Entry<MemberId, Long> entry : decrements.entrySet()) {
      this.decrements.accumulateAndGet(entry.getKey(), entry.getValue(), Math::max);
    }
  }

  private void broadcastCounters() {
    List<Map<MemberId, Long>> changes = Lists.newArrayList(Maps.newHashMap(increments.asMap()), Maps.newHashMap(decrements.asMap()));
    clusterCommunicator.broadcast(subject, changes, SERIALIZER::encode);
  }

  @Override
  public void close() {
    broadcastFuture.cancel(false);
    clusterCommunicator.unsubscribe(subject);
  }
}
