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
package io.atomix.primitives.elector.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import io.atomix.cluster.NodeId;
import io.atomix.leadership.Leadership;
import io.atomix.primitives.elector.AsyncLeaderElector;
import io.atomix.primitives.elector.LeaderElectionEvent;
import io.atomix.primitives.elector.LeaderElectorEventListener;
import io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.Anoint;
import io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.GetElectedTopics;
import io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.GetLeadership;
import io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.Promote;
import io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.Run;
import io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.Withdraw;
import io.atomix.primitives.impl.AbstractRaftPrimitive;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.atomix.primitives.elector.impl.RaftLeaderElectorEvents.CHANGE;
import static io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.ADD_LISTENER;
import static io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.ANOINT;
import static io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.EVICT;
import static io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.GET_ALL_LEADERSHIPS;
import static io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.GET_ELECTED_TOPICS;
import static io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.GET_LEADERSHIP;
import static io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.PROMOTE;
import static io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.REMOVE_LISTENER;
import static io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.RUN;
import static io.atomix.primitives.elector.impl.RaftLeaderElectorOperations.WITHDRAW;

/**
 * Distributed resource providing the {@link AsyncLeaderElector} primitive.
 */
public class RaftLeaderElector extends AbstractRaftPrimitive implements AsyncLeaderElector {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .register(NodeId.class)
      .register(RaftLeaderElectorOperations.NAMESPACE)
      .register(RaftLeaderElectorEvents.NAMESPACE)
      .build());

  private final Set<LeaderElectorEventListener> leadershipChangeListeners = Sets.newCopyOnWriteArraySet();
  private final LeaderElectorEventListener cacheUpdater;
  private final Consumer<Status> statusListener;

  private final LoadingCache<String, CompletableFuture<Leadership>> cache;

  public RaftLeaderElector(RaftProxy proxy) {
    super(proxy);
    cache = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .build(CacheLoader.from(topic -> proxy.invoke(
            GET_LEADERSHIP, SERIALIZER::encode, new GetLeadership(topic), SERIALIZER::decode)));

    cacheUpdater = change -> {
      Leadership leadership = change.newLeadership();
      cache.put(leadership.topic(), CompletableFuture.completedFuture(leadership));
    };
    statusListener = status -> {
      if (status == Status.SUSPENDED || status == Status.INACTIVE) {
        cache.invalidateAll();
      }
    };
    addStatusChangeListener(statusListener);

    proxy.addStateChangeListener(state -> {
      if (state == RaftProxy.State.CONNECTED && isListening()) {
        proxy.invoke(ADD_LISTENER);
      }
    });
    proxy.addEventListener(CHANGE, SERIALIZER::decode, this::handleEvent);
  }

  @Override
  public CompletableFuture<Void> destroy() {
    removeStatusChangeListener(statusListener);
    return removeListener(cacheUpdater);
  }

  public CompletableFuture<RaftLeaderElector> setupCache() {
    return addListener(cacheUpdater).thenApply(v -> this);
  }

  private void handleEvent(List<LeaderElectionEvent> changes) {
    changes.forEach(change -> leadershipChangeListeners.forEach(l -> l.onEvent(change)));
  }

  @Override
  public CompletableFuture<Leadership> run(String topic, NodeId nodeId) {
    return proxy.<Run, Leadership>invoke(RUN, SERIALIZER::encode, new Run(topic, nodeId), SERIALIZER::decode)
        .whenComplete((r, e) -> cache.invalidate(topic));
  }

  @Override
  public CompletableFuture<Void> withdraw(String topic) {
    return proxy.invoke(WITHDRAW, SERIALIZER::encode, new Withdraw(topic))
        .whenComplete((r, e) -> cache.invalidate(topic));
  }

  @Override
  public CompletableFuture<Boolean> anoint(String topic, NodeId nodeId) {
    return proxy.<Anoint, Boolean>invoke(ANOINT, SERIALIZER::encode, new Anoint(topic, nodeId), SERIALIZER::decode)
        .whenComplete((r, e) -> cache.invalidate(topic));
  }

  @Override
  public CompletableFuture<Boolean> promote(String topic, NodeId nodeId) {
    return proxy.<Promote, Boolean>invoke(
        PROMOTE, SERIALIZER::encode, new Promote(topic, nodeId), SERIALIZER::decode)
        .whenComplete((r, e) -> cache.invalidate(topic));
  }

  @Override
  public CompletableFuture<Void> evict(NodeId nodeId) {
    return proxy.invoke(EVICT, SERIALIZER::encode, new RaftLeaderElectorOperations.Evict(nodeId));
  }

  @Override
  public CompletableFuture<Leadership> getLeadership(String topic) {
    return cache.getUnchecked(topic)
        .whenComplete((r, e) -> {
          if (e != null) {
            cache.invalidate(topic);
          }
        });
  }

  @Override
  public CompletableFuture<Map<String, Leadership>> getLeaderships() {
    return proxy.invoke(GET_ALL_LEADERSHIPS, SERIALIZER::decode);
  }

  public CompletableFuture<Set<String>> getElectedTopics(NodeId nodeId) {
    return proxy.invoke(GET_ELECTED_TOPICS, SERIALIZER::encode, new GetElectedTopics(nodeId), SERIALIZER::decode);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(LeaderElectorEventListener listener) {
    if (leadershipChangeListeners.isEmpty()) {
      return proxy.invoke(ADD_LISTENER).thenRun(() -> leadershipChangeListeners.add(listener));
    } else {
      leadershipChangeListeners.add(listener);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(LeaderElectorEventListener listener) {
    if (leadershipChangeListeners.remove(listener) && leadershipChangeListeners.isEmpty()) {
      return proxy.invoke(REMOVE_LISTENER).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private boolean isListening() {
    return !leadershipChangeListeners.isEmpty();
  }
}