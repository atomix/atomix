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
import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.Serializer;

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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed resource providing the {@link AsyncLeaderElector} primitive.
 */
public class LeaderElectorProxy extends AbstractAsyncPrimitive implements AsyncLeaderElector<byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(LeaderElectorOperations.NAMESPACE)
      .register(LeaderElectorEvents.NAMESPACE)
      .build());

  private final Set<LeadershipEventListener<byte[]>> leadershipChangeListeners = Sets.newCopyOnWriteArraySet();

  public LeaderElectorProxy(PrimitiveProxy proxy) {
    super(proxy);
    proxy.addStateChangeListener(state -> {
      if (state == PrimitiveProxy.State.CONNECTED && isListening()) {
        proxy.invoke(ADD_LISTENER);
      }
    });
    proxy.addEventListener(CHANGE, SERIALIZER::decode, this::handleEvent);
  }

  private void handleEvent(List<LeadershipEvent<byte[]>> changes) {
    changes.forEach(change -> leadershipChangeListeners.forEach(l -> l.onEvent(change)));
  }

  @Override
  public CompletableFuture<Leadership<byte[]>> run(String topic, byte[] id) {
    return proxy.invoke(RUN, SERIALIZER::encode, new Run(topic, id), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> withdraw(String topic, byte[] id) {
    return proxy.invoke(WITHDRAW, SERIALIZER::encode, new Withdraw(topic, id));
  }

  @Override
  public CompletableFuture<Boolean> anoint(String topic, byte[] id) {
    return proxy.invoke(ANOINT, SERIALIZER::encode, new Anoint(topic, id), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> promote(String topic, byte[] id) {
    return proxy.invoke(PROMOTE, SERIALIZER::encode, new Promote(topic, id), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> evict(byte[] id) {
    return proxy.invoke(EVICT, SERIALIZER::encode, new Evict(id));
  }

  @Override
  public CompletableFuture<Leadership<byte[]>> getLeadership(String topic) {
    return proxy.invoke(GET_LEADERSHIP, SERIALIZER::encode, new GetLeadership(topic), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Map<String, Leadership<byte[]>>> getLeaderships() {
    return proxy.invoke(GET_ALL_LEADERSHIPS, SERIALIZER::decode);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(LeadershipEventListener<byte[]> listener) {
    if (leadershipChangeListeners.isEmpty()) {
      return proxy.invoke(ADD_LISTENER).thenRun(() -> leadershipChangeListeners.add(listener));
    } else {
      leadershipChangeListeners.add(listener);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(LeadershipEventListener<byte[]> listener) {
    if (leadershipChangeListeners.remove(listener) && leadershipChangeListeners.isEmpty()) {
      return proxy.invoke(REMOVE_LISTENER).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private boolean isListening() {
    return !leadershipChangeListeners.isEmpty();
  }

  @Override
  public LeaderElector<byte[]> sync(Duration operationTimeout) {
    return new BlockingLeaderElector<>(this, operationTimeout.toMillis());
  }
}