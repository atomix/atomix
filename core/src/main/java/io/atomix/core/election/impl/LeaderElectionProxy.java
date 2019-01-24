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

import com.google.common.collect.Sets;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.ProxyClient;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed resource providing the {@link AsyncLeaderElection} primitive.
 */
public class LeaderElectionProxy
    extends AbstractAsyncPrimitive<AsyncLeaderElection<byte[]>, LeaderElectionService>
    implements AsyncLeaderElection<byte[]>, LeaderElectionClient {

  private final Set<LeadershipEventListener<byte[]>> leadershipChangeListeners = Sets.newCopyOnWriteArraySet();

  public LeaderElectionProxy(ProxyClient<LeaderElectionService> proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public void onLeadershipChange(Leadership<byte[]> oldLeadership, Leadership<byte[]> newLeadership) {
    leadershipChangeListeners.forEach(l -> l.event(
        new LeadershipEvent<>(LeadershipEvent.Type.CHANGE, name(), oldLeadership, newLeadership)));
  }

  @Override
  public CompletableFuture<Leadership<byte[]>> run(byte[] id) {
    return getProxyClient().applyBy(name(), service -> service.run(id));
  }

  @Override
  public CompletableFuture<Void> withdraw(byte[] id) {
    return getProxyClient().acceptBy(name(), service -> service.withdraw(id));
  }

  @Override
  public CompletableFuture<Boolean> anoint(byte[] id) {
    return getProxyClient().applyBy(name(), service -> service.anoint(id));
  }

  @Override
  public CompletableFuture<Boolean> promote(byte[] id) {
    return getProxyClient().applyBy(name(), service -> service.promote(id));
  }

  @Override
  public CompletableFuture<Void> evict(byte[] id) {
    return getProxyClient().acceptBy(name(), service -> service.evict(id));
  }

  @Override
  public CompletableFuture<Leadership<byte[]>> getLeadership() {
    return getProxyClient().applyBy(name(), service -> service.getLeadership());
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(LeadershipEventListener<byte[]> listener) {
    if (leadershipChangeListeners.isEmpty()) {
      return getProxyClient().acceptBy(name(), service -> service.listen()).thenRun(() -> leadershipChangeListeners.add(listener));
    } else {
      leadershipChangeListeners.add(listener);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(LeadershipEventListener<byte[]> listener) {
    if (leadershipChangeListeners.remove(listener) && leadershipChangeListeners.isEmpty()) {
      return getProxyClient().acceptBy(name(), service -> service.unlisten()).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private boolean isListening() {
    return !leadershipChangeListeners.isEmpty();
  }

  @Override
  public CompletableFuture<AsyncLeaderElection<byte[]>> connect() {
    return super.connect()
        .thenCompose(v -> getProxyClient().getPartition(name()).connect())
        .thenRun(() -> getProxyClient().getPartitions().forEach(partition -> {
          partition.addStateChangeListener(state -> {
            if (state == PrimitiveState.CONNECTED && isListening()) {
              partition.accept(service -> service.listen());
            }
          });
        }))
        .thenApply(v -> this);
  }

  @Override
  public LeaderElection<byte[]> sync(Duration operationTimeout) {
    return new BlockingLeaderElection<>(this, operationTimeout.toMillis());
  }
}
