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
package io.atomix.core.election.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.core.election.AsyncLeaderElector;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.core.election.impl.LeaderElectorOperations.Anoint;
import io.atomix.core.election.impl.LeaderElectorOperations.Evict;
import io.atomix.core.election.impl.LeaderElectorOperations.GetLeadership;
import io.atomix.core.election.impl.LeaderElectorOperations.Promote;
import io.atomix.core.election.impl.LeaderElectorOperations.Run;
import io.atomix.core.election.impl.LeaderElectorOperations.Withdraw;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.Proxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.atomix.core.election.impl.LeaderElectorEvents.CHANGE;
import static io.atomix.core.election.impl.LeaderElectorOperations.ADD_LISTENER;
import static io.atomix.core.election.impl.LeaderElectorOperations.ANOINT;
import static io.atomix.core.election.impl.LeaderElectorOperations.EVICT;
import static io.atomix.core.election.impl.LeaderElectorOperations.GET_ALL_LEADERSHIPS;
import static io.atomix.core.election.impl.LeaderElectorOperations.GET_LEADERSHIP;
import static io.atomix.core.election.impl.LeaderElectorOperations.PROMOTE;
import static io.atomix.core.election.impl.LeaderElectorOperations.REMOVE_LISTENER;
import static io.atomix.core.election.impl.LeaderElectorOperations.RUN;
import static io.atomix.core.election.impl.LeaderElectorOperations.WITHDRAW;

/**
 * Distributed resource providing the {@link AsyncLeaderElector} primitive.
 */
public class LeaderElectorProxy extends AbstractAsyncPrimitive<AsyncLeaderElector<byte[]>> implements AsyncLeaderElector<byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(LeaderElectorOperations.NAMESPACE)
      .register(LeaderElectorEvents.NAMESPACE)
      .build());

  private final Map<String, Set<LeadershipEventListener<byte[]>>> leadershipChangeListeners = Maps.newConcurrentMap();

  public LeaderElectorProxy(PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  private void handleEvent(PartitionId partitionId, List<LeadershipEvent<byte[]>> changes) {
    changes.forEach(change -> {
      Set<LeadershipEventListener<byte[]>> listenerSet = leadershipChangeListeners.get(change.topic());
      if (listenerSet != null) {
        listenerSet.forEach(l -> l.onEvent(change));
      }
    });
  }

  @Override
  protected Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public CompletableFuture<Leadership<byte[]>> run(String topic, byte[] id) {
    return invokeBy(topic, RUN, new Run(topic, id));
  }

  @Override
  public CompletableFuture<Void> withdraw(String topic, byte[] id) {
    return invokeBy(topic, WITHDRAW, new Withdraw(topic, id));
  }

  @Override
  public CompletableFuture<Boolean> anoint(String topic, byte[] id) {
    return invokeBy(topic, ANOINT, new Anoint(topic, id));
  }

  @Override
  public CompletableFuture<Boolean> promote(String topic, byte[] id) {
    return invokeBy(topic, PROMOTE, new Promote(topic, id));
  }

  @Override
  public CompletableFuture<Void> evict(byte[] id) {
    return invokeAll(EVICT, new Evict(id)).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Leadership<byte[]>> getLeadership(String topic) {
    return invokeBy(topic, GET_LEADERSHIP, new GetLeadership(topic));
  }

  @Override
  public CompletableFuture<Map<String, Leadership<byte[]>>> getLeaderships() {
    return this.<Map<String, Leadership<byte[]>>>invokeAll(GET_ALL_LEADERSHIPS)
        .thenApply(leaderships -> {
          ImmutableMap.Builder<String, Leadership<byte[]>> builder = ImmutableMap.builder();
          leaderships.forEach(builder::putAll);
          return builder.build();
        });
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(String topic, LeadershipEventListener<byte[]> listener) {
    boolean empty = leadershipChangeListeners.isEmpty();
    leadershipChangeListeners.compute(topic, (t, s) -> {
      if (s == null) {
        s = Sets.newCopyOnWriteArraySet();
      }
      s.add(listener);
      return s;
    });

    if (empty) {
      return invokeBy(topic, ADD_LISTENER);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(String topic, LeadershipEventListener<byte[]> listener) {
    leadershipChangeListeners.computeIfPresent(topic, (t, s) -> {
      s.remove(listener);
      return s.size() == 0 ? null : s;
    });
    if (leadershipChangeListeners.isEmpty()) {
      return invokeBy(topic, REMOVE_LISTENER).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private boolean isListening() {
    return !leadershipChangeListeners.isEmpty();
  }

  @Override
  public CompletableFuture<AsyncLeaderElector<byte[]>> connect() {
    return super.connect()
        .thenRun(() -> {
          addStateChangeListeners((partition, state) -> {
            if (state == Proxy.State.CONNECTED && isListening()) {
              invokeOn(partition, ADD_LISTENER);
            }
          });
          listenAll(CHANGE, this::handleEvent);
        })
        .thenApply(v -> this);
  }

  @Override
  public LeaderElector<byte[]> sync(Duration operationTimeout) {
    return new BlockingLeaderElector<>(this, operationTimeout.toMillis());
  }
}