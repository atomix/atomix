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

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.counter.CounterProtocol;
import io.atomix.protocols.gossip.CrdtProtocolConfig;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * CRDT based counter implementation.
 */
public class CrdtCounter implements CounterProtocol {
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .register(MemberId.class)
      .build());

  private final ClusterMembershipService clusterMembershipService;
  private final ClusterCommunicationService clusterCommunicator;
  private final ScheduledExecutorService executorService;
  private final String subject;
  private volatile ScheduledFuture<?> broadcastFuture;
  private final Map<MemberId, Integer> counters = Maps.newConcurrentMap();

  public CrdtCounter(String name, CrdtProtocolConfig config, PrimitiveManagementService managementService) {
    this.clusterMembershipService = managementService.getMembershipService();
    this.clusterCommunicator = managementService.getCommunicationService();
    this.executorService = managementService.getExecutorService();
    this.subject = String.format("atomix-crdt-counter-%s", name);
    clusterCommunicator.subscribe(subject, SERIALIZER::decode, this::updateCounters, executorService);
    broadcastFuture = executorService.scheduleAtFixedRate(
        this::broadcastCounters, config.getGossipInterval().toMillis(), config.getGossipInterval().toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public long get() {
    return counters.values().stream().mapToLong(v -> v).sum();
  }

  @Override
  public long increment() {
    counters.compute(clusterMembershipService.getLocalMember().id(), (id, value) -> value != null ? value + 1 : 1);
    broadcastCounters();
    return get();
  }

  private void updateCounters(Map<MemberId, Integer> counters) {
    for (Map.Entry<MemberId, Integer> entry : counters.entrySet()) {
      this.counters.compute(entry.getKey(), (key, value) -> {
        if (value == null || value < entry.getValue()) {
          return entry.getValue();
        }
        return value;
      });
    }
  }

  private void broadcastCounters() {
    clusterCommunicator.broadcast(subject, counters, SERIALIZER::encode);
  }

  @Override
  public void close() {
    broadcastFuture.cancel(false);
    clusterCommunicator.unsubscribe(subject);
  }
}
