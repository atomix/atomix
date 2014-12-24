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
import java.util.concurrent.Executor;

/**
 * Internal Copycat implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycat implements Copycat {
  private final ClusterCoordinator coordinator;
  private final ClusterConfig cluster;
  private final CopycatConfig config;
  private final Executor executor;

  public DefaultCopycat(String uri, ClusterConfig cluster, CopycatConfig config, Executor executor) {
    this.coordinator = new DefaultClusterCoordinator(uri, cluster, ExecutionContext.create());
    this.cluster = cluster;
    this.config = config;
    this.executor = executor;
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name) {
    return eventLog(name, cluster, new EventLogConfig().withSerializer(config.getSerializer()), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, Executor executor) {
    return eventLog(name, cluster, new EventLogConfig().withSerializer(config.getSerializer()), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster) {
    return eventLog(name, cluster, new EventLogConfig().withSerializer(config.getSerializer()), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster, Executor executor) {
    return eventLog(name, cluster, new EventLogConfig().withSerializer(config.getSerializer()), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, EventLogConfig config) {
    return eventLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, EventLogConfig config, Executor executor) {
    return eventLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster, EventLogConfig config) {
    return eventLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster, EventLogConfig config, Executor executor) {
    return coordinator.createResource(name, cluster).thenApplyAsync(context -> new DefaultEventLog<T>(name, context, coordinator, config, executor), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name) {
    return stateLog(name, cluster, new StateLogConfig().withSerializer(config.getSerializer()), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, Executor executor) {
    return stateLog(name, cluster, new StateLogConfig().withSerializer(config.getSerializer()), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster) {
    return stateLog(name, cluster, new StateLogConfig().withSerializer(config.getSerializer()), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster, Executor executor) {
    return stateLog(name, cluster, new StateLogConfig().withSerializer(config.getSerializer()), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, StateLogConfig config) {
    return stateLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, StateLogConfig config, Executor executor) {
    return stateLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster, StateLogConfig config) {
    return stateLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster, StateLogConfig config, Executor executor) {
    return coordinator.createResource(name, cluster).thenApplyAsync(context -> new DefaultStateLog<T>(name, context, coordinator, config, executor), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState) {
    return stateMachine(name, stateType, initialState, cluster, new StateMachineConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, Executor executor) {
    return stateMachine(name, stateType, initialState, cluster, new StateMachineConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster) {
    return stateMachine(name, stateType, initialState, cluster, new StateMachineConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster, Executor executor) {
    return stateMachine(name, stateType, initialState, cluster, new StateMachineConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, StateMachineConfig config) {
    return stateMachine(name, stateType, initialState, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, StateMachineConfig config, Executor executor) {
    return stateMachine(name, stateType, initialState, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster, StateMachineConfig config) {
    return stateMachine(name, stateType, initialState, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster, StateMachineConfig config, Executor executor) {
    return this.<List<Object>>stateLog(name, cluster, config).thenApplyAsync(log -> new DefaultStateMachine<T>(stateType, initialState, log), executor);
  }

  @Override
  public synchronized CompletableFuture<LeaderElection> election(String name) {
    return election(name, cluster, executor);
  }

  @Override
  public synchronized CompletableFuture<LeaderElection> election(String name, Executor executor) {
    return election(name, cluster, executor);
  }

  @Override
  public synchronized CompletableFuture<LeaderElection> election(String name, ClusterConfig cluster) {
    return election(name, cluster, executor);
  }

  @Override
  public synchronized CompletableFuture<LeaderElection> election(String name, ClusterConfig cluster, Executor executor) {
    return coordinator.createResource(name, cluster).thenApplyAsync(context -> new DefaultLeaderElection(name, context, coordinator, executor), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name) {
    return map(name, cluster, new AsyncMapConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, Executor executor) {
    return map(name, cluster, new AsyncMapConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster) {
    return map(name, cluster, new AsyncMapConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster, Executor executor) {
    return map(name, cluster, new AsyncMapConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, AsyncMapConfig config) {
    return map(name, cluster, config, executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, AsyncMapConfig config, Executor executor) {
    return map(name, cluster, config, executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster, AsyncMapConfig config) {
    return map(name, cluster, config, executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster, AsyncMapConfig config, Executor executor) {
    return stateMachine(name, AsyncMapState.class, new DefaultAsyncMapState<>(), cluster, config, executor).thenApplyAsync(stateMachine -> new DefaultAsyncMap(stateMachine), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name) {
    return multiMap(name, cluster, new AsyncMultiMapConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, Executor executor) {
    return multiMap(name, cluster, new AsyncMultiMapConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster) {
    return multiMap(name, cluster, new AsyncMultiMapConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster, Executor executor) {
    return multiMap(name, cluster, new AsyncMultiMapConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, AsyncMultiMapConfig config) {
    return multiMap(name, cluster, config, executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, AsyncMultiMapConfig config, Executor executor) {
    return multiMap(name, cluster, config, executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster, AsyncMultiMapConfig config) {
    return multiMap(name, cluster, config, executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster, AsyncMultiMapConfig config, Executor executor) {
    return stateMachine(name, AsyncMultiMapState.class, new DefaultAsyncMultiMapState<>(), cluster, config, executor).thenApplyAsync(stateMachine -> new DefaultAsyncMultiMap(stateMachine), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name) {
    return list(name, cluster, new AsyncListConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, Executor executor) {
    return list(name, cluster, new AsyncListConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster) {
    return list(name, cluster, new AsyncListConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster, Executor executor) {
    return list(name, cluster, new AsyncListConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, AsyncListConfig config) {
    return list(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, AsyncListConfig config, Executor executor) {
    return list(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster, AsyncListConfig config) {
    return list(name, cluster, config, executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster, AsyncListConfig config, Executor executor) {
    return stateMachine(name, AsyncListState.class, new DefaultAsyncListState<>(), cluster, config, executor).thenApplyAsync(stateMachine -> new DefaultAsyncList(stateMachine), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name) {
    return set(name, cluster, new AsyncSetConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, Executor executor) {
    return set(name, cluster, new AsyncSetConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster) {
    return set(name, cluster, new AsyncSetConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster, Executor executor) {
    return set(name, cluster, new AsyncSetConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, AsyncSetConfig config) {
    return set(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, AsyncSetConfig config, Executor executor) {
    return set(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster, AsyncSetConfig config) {
    return set(name, cluster, config, executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster, AsyncSetConfig config, Executor executor) {
    return stateMachine(name, AsyncSetState.class, new DefaultAsyncSetState<>(), cluster, config, executor).thenApplyAsync(stateMachine -> new DefaultAsyncSet(stateMachine), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name) {
    return lock(name, cluster, new AsyncLockConfig(), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, Executor executor) {
    return lock(name, cluster, new AsyncLockConfig(), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster) {
    return lock(name, cluster, new AsyncLockConfig(), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster, Executor executor) {
    return lock(name, cluster, new AsyncLockConfig(), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, AsyncLockConfig config) {
    return lock(name, cluster, config, executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, AsyncLockConfig config, Executor executor) {
    return lock(name, cluster, config, executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster, AsyncLockConfig config) {
    return lock(name, cluster, config, executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster, AsyncLockConfig config, Executor executor) {
    return stateMachine(name, AsyncLockState.class, new UnlockedAsyncLockState(), cluster, config, executor).thenApplyAsync(DefaultAsyncLock::new, executor);
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
