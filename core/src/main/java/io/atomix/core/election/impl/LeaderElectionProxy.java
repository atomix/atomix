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

import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.core.election.impl.LeaderElectionOperations.Anoint;
import io.atomix.core.election.impl.LeaderElectionOperations.Evict;
import io.atomix.core.election.impl.LeaderElectionOperations.Promote;
import io.atomix.core.election.impl.LeaderElectionOperations.Run;
import io.atomix.core.election.impl.LeaderElectionOperations.Withdraw;
import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.Serializer;

import static io.atomix.core.election.impl.LeaderElectionEvents.CHANGE;
import static io.atomix.core.election.impl.LeaderElectionOperations.ADD_LISTENER;
import static io.atomix.core.election.impl.LeaderElectionOperations.ANOINT;
import static io.atomix.core.election.impl.LeaderElectionOperations.EVICT;
import static io.atomix.core.election.impl.LeaderElectionOperations.GET_LEADERSHIP;
import static io.atomix.core.election.impl.LeaderElectionOperations.PROMOTE;
import static io.atomix.core.election.impl.LeaderElectionOperations.REMOVE_LISTENER;
import static io.atomix.core.election.impl.LeaderElectionOperations.RUN;
import static io.atomix.core.election.impl.LeaderElectionOperations.WITHDRAW;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed resource providing the {@link AsyncLeaderElection} primitive.
 */
public class LeaderElectionProxy extends AbstractAsyncPrimitive implements AsyncLeaderElection<byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(LeaderElectionOperations.NAMESPACE)
      .register(LeaderElectionEvents.NAMESPACE)
      .build());

  private final Set<LeadershipEventListener> leadershipChangeListeners = Sets.newCopyOnWriteArraySet();

  public LeaderElectionProxy(PrimitiveProxy proxy) {
    super(proxy);
    proxy.addStateChangeListener(state -> {
      if (state == PrimitiveProxy.State.CONNECTED && isListening()) {
        proxy.invoke(ADD_LISTENER);
      }
    });
    proxy.addEventListener(CHANGE, SERIALIZER::decode, this::handleEvent);
  }

  private void handleEvent(List<LeadershipEvent> changes) {
    changes.forEach(change -> leadershipChangeListeners.forEach(l -> l.onEvent(change)));
  }

  @Override
  public CompletableFuture<Leadership<byte[]>> run(byte[] id) {
    return proxy.invoke(RUN, SERIALIZER::encode, new Run(id), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> withdraw(byte[] id) {
    return proxy.invoke(WITHDRAW, SERIALIZER::encode, new Withdraw(id));
  }

  @Override
  public CompletableFuture<Boolean> anoint(byte[] id) {
    return proxy.<Anoint, Boolean>invoke(ANOINT, SERIALIZER::encode, new Anoint(id), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> promote(byte[] id) {
    return proxy.<Promote, Boolean>invoke(PROMOTE, SERIALIZER::encode, new Promote(id), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> evict(byte[] id) {
    return proxy.invoke(EVICT, SERIALIZER::encode, new Evict(id));
  }

  @Override
  public CompletableFuture<Leadership<byte[]>> getLeadership() {
    return proxy.invoke(GET_LEADERSHIP, SERIALIZER::decode);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(LeadershipEventListener listener) {
    if (leadershipChangeListeners.isEmpty()) {
      return proxy.invoke(ADD_LISTENER).thenRun(() -> leadershipChangeListeners.add(listener));
    } else {
      leadershipChangeListeners.add(listener);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(LeadershipEventListener listener) {
    if (leadershipChangeListeners.remove(listener) && leadershipChangeListeners.isEmpty()) {
      return proxy.invoke(REMOVE_LISTENER).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private boolean isListening() {
    return !leadershipChangeListeners.isEmpty();
  }

  @Override
  public LeaderElection<byte[]> sync(Duration operationTimeout) {
    return new BlockingLeaderElection<>(this, operationTimeout.toMillis());
  }
}