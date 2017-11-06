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
package io.atomix.primitives.leadership.impl;

import com.google.common.collect.Sets;
import io.atomix.primitives.impl.AbstractRaftPrimitive;
import io.atomix.primitives.leadership.AsyncLeaderElector;
import io.atomix.primitives.leadership.Leadership;
import io.atomix.primitives.leadership.LeadershipEvent;
import io.atomix.primitives.leadership.LeadershipEventListener;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.Anoint;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.Evict;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.Promote;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.Run;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.atomix.primitives.leadership.impl.RaftLeaderElectorEvents.CHANGE;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.ADD_LISTENER;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.ANOINT;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.EVICT;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.GET_LEADERSHIP;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.PROMOTE;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.REMOVE_LISTENER;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.RUN;
import static io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations.WITHDRAW;

/**
 * Distributed resource providing the {@link AsyncLeaderElector} primitive.
 */
public class RaftLeaderElector extends AbstractRaftPrimitive implements AsyncLeaderElector<byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
      .register(RaftLeaderElectorOperations.NAMESPACE)
      .register(RaftLeaderElectorEvents.NAMESPACE)
      .build());

  private final Set<LeadershipEventListener> leadershipChangeListeners = Sets.newCopyOnWriteArraySet();

  public RaftLeaderElector(RaftProxy proxy) {
    super(proxy);
    proxy.addStateChangeListener(state -> {
      if (state == RaftProxy.State.CONNECTED && isListening()) {
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
  public CompletableFuture<Void> withdraw() {
    return proxy.invoke(WITHDRAW);
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
}