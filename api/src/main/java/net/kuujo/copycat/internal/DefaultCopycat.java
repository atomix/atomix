/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal;

import net.kuujo.copycat.*;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.collections.*;
import net.kuujo.copycat.collections.internal.collection.*;
import net.kuujo.copycat.collections.internal.lock.AsyncLockState;
import net.kuujo.copycat.collections.internal.lock.DefaultAsyncLock;
import net.kuujo.copycat.collections.internal.lock.UnlockedAsyncLockState;
import net.kuujo.copycat.collections.internal.map.*;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.election.internal.DefaultLeaderElection;
import net.kuujo.copycat.internal.cluster.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Internal Copycat implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycat implements Copycat {
  private final ClusterCoordinator coordinator;
  private final ClusterConfig cluster;
  private final CopycatConfig config;
  private final ExecutionContext executor;

  public DefaultCopycat(String uri, ClusterConfig cluster, CopycatConfig config, ExecutionContext executor) {
    this.coordinator = new DefaultClusterCoordinator(uri, cluster, ExecutionContext.create());
    this.cluster = cluster;
    this.config = config;
    this.executor = executor;
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name) {
    return eventLog(name, cluster, new EventLogConfig().withSerializer(config.getSerializer()));
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster) {
    return eventLog(name, cluster, new EventLogConfig().withSerializer(config.getSerializer()));
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, EventLogConfig config) {
    return eventLog(name, cluster, config);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster, EventLogConfig config) {
    return coordinator.createResource(name, cluster).thenApplyAsync(context -> new DefaultEventLog<T>(name, context, coordinator, config, executor), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name) {
    return stateLog(name, cluster, new StateLogConfig().withSerializer(config.getSerializer()));
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster) {
    return stateLog(name, cluster, new StateLogConfig().withSerializer(config.getSerializer()));
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, StateLogConfig config) {
    return stateLog(name, cluster, config);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster, StateLogConfig config) {
    return coordinator.createResource(name, cluster).thenApplyAsync(context -> new DefaultStateLog<T>(name, context, coordinator, config, executor), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState) {
    return stateMachine(name, stateType, initialState, cluster, new StateMachineConfig());
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster) {
    return stateMachine(name, stateType, initialState, cluster, new StateMachineConfig());
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, StateMachineConfig config) {
    return stateMachine(name, stateType, initialState, cluster, config);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster, StateMachineConfig config) {
    return this.<List<Object>>stateLog(name, cluster, config).thenApplyAsync(log -> new DefaultStateMachine<T>(stateType, initialState, log), executor);
  }

  @Override
  public synchronized CompletableFuture<LeaderElection> election(String name) {
    return election(name, cluster);
  }

  @Override
  public synchronized CompletableFuture<LeaderElection> election(String name, ClusterConfig cluster) {
    return coordinator.createResource(name, cluster).thenApplyAsync(context -> new DefaultLeaderElection(name, context, coordinator, executor), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> getMap(String name) {
    return getMap(name, cluster, new AsyncMapConfig());
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> getMap(String name, ClusterConfig cluster) {
    return getMap(name, cluster, new AsyncMapConfig());
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> getMap(String name, AsyncMapConfig config) {
    return getMap(name, cluster, config);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> getMap(String name, ClusterConfig cluster, AsyncMapConfig config) {
    return stateMachine(name, AsyncMapState.class, new DefaultAsyncMapState<>(), cluster, config).thenApplyAsync(stateMachine -> new DefaultAsyncMap(stateMachine), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> getMultiMap(String name) {
    return getMultiMap(name, cluster, new AsyncMultiMapConfig());
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> getMultiMap(String name, ClusterConfig cluster) {
    return getMultiMap(name, cluster, new AsyncMultiMapConfig());
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> getMultiMap(String name, AsyncMultiMapConfig config) {
    return getMultiMap(name, cluster, config);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> getMultiMap(String name, ClusterConfig cluster, AsyncMultiMapConfig config) {
    return stateMachine(name, AsyncMultiMapState.class, new DefaultAsyncMultiMapState<>(), cluster, config).thenApplyAsync(stateMachine -> new DefaultAsyncMultiMap(stateMachine), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> getList(String name) {
    return getList(name, cluster, new AsyncListConfig());
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> getList(String name, ClusterConfig cluster) {
    return getList(name, cluster, new AsyncListConfig());
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> getList(String name, AsyncListConfig config) {
    return getList(name, cluster, config);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> CompletableFuture<AsyncList<T>> getList(String name, ClusterConfig cluster, AsyncListConfig config) {
    return stateMachine(name, AsyncListState.class, new DefaultAsyncListState<>(), cluster, config).thenApplyAsync(stateMachine -> new DefaultAsyncList(stateMachine), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> getSet(String name) {
    return getSet(name, cluster, new AsyncSetConfig());
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> getSet(String name, ClusterConfig cluster) {
    return getSet(name, cluster, new AsyncSetConfig());
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> getSet(String name, AsyncSetConfig config) {
    return getSet(name, cluster, config);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> CompletableFuture<AsyncSet<T>> getSet(String name, ClusterConfig cluster, AsyncSetConfig config) {
    return stateMachine(name, AsyncSetState.class, new DefaultAsyncSetState<>(), cluster, config).thenApplyAsync(stateMachine -> new DefaultAsyncSet(stateMachine), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> getLock(String name) {
    return getLock(name, cluster, new AsyncLockConfig());
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> getLock(String name, ClusterConfig cluster) {
    return getLock(name, cluster, new AsyncLockConfig());
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> getLock(String name, AsyncLockConfig config) {
    return getLock(name, cluster, config);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> getLock(String name, ClusterConfig cluster, AsyncLockConfig config) {
    return stateMachine(name, AsyncLockState.class, new UnlockedAsyncLockState(), cluster, config).thenApplyAsync(DefaultAsyncLock::new, executor);
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    return coordinator.open();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return coordinator.close();
  }

}
