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

import net.kuujo.copycat.Copycat;
import net.kuujo.copycat.EventLog;
import net.kuujo.copycat.StateLog;
import net.kuujo.copycat.StateMachine;
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
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.log.LogConfig;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Internal Copycat implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycat implements Copycat {
  private final ClusterCoordinator coordinator;
  private final ClusterConfig cluster;
  private final LogConfig config;
  private final Executor executor;

  public DefaultCopycat(String uri, ClusterConfig cluster, LogConfig config, Executor executor) {
    this.coordinator = new DefaultClusterCoordinator(uri, cluster, Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-coordinator-%d")));
    this.cluster = cluster;
    this.config = config;
    this.executor = executor;
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name) {
    return eventLog(name, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, Executor executor) {
    return eventLog(name, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster) {
    return eventLog(name, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster, Executor executor) {
    return eventLog(name, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, LogConfig config) {
    return eventLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, LogConfig config, Executor executor) {
    return eventLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster, LogConfig config) {
    return eventLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster, LogConfig config, Executor executor) {
    return coordinator.createResource(name, cluster, config).thenApplyAsync(context -> new DefaultEventLog<T>(name, context, coordinator, executor), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name) {
    return stateLog(name, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, Executor executor) {
    return stateLog(name, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster) {
    return stateLog(name, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster, Executor executor) {
    return stateLog(name, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, LogConfig config) {
    return stateLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, LogConfig config, Executor executor) {
    return stateLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster, LogConfig config) {
    return stateLog(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster, LogConfig config, Executor executor) {
    return coordinator.createResource(name, cluster).thenApplyAsync(context -> new DefaultStateLog<T>(name, context, coordinator, config, executor), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState) {
    return stateMachine(name, stateType, initialState, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, Executor executor) {
    return stateMachine(name, stateType, initialState, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster) {
    return stateMachine(name, stateType, initialState, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster, Executor executor) {
    return stateMachine(name, stateType, initialState, cluster, config.copy(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, LogConfig config) {
    return stateMachine(name, stateType, initialState, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, LogConfig config, Executor executor) {
    return stateMachine(name, stateType, initialState, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster, LogConfig config) {
    return stateMachine(name, stateType, initialState, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster, LogConfig config, Executor executor) {
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
    return map(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, Executor executor) {
    return map(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster) {
    return map(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster, Executor executor) {
    return map(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, LogConfig config) {
    return map(name, cluster, config, executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, LogConfig config, Executor executor) {
    return map(name, cluster, config, executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster, LogConfig config) {
    return map(name, cluster, config, executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster, LogConfig config, Executor executor) {
    return stateMachine(name, MapState.class, new DefaultMapState<>(), cluster, config, executor).thenApplyAsync(stateMachine -> new DefaultAsyncMap(stateMachine), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name) {
    return multiMap(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, Executor executor) {
    return multiMap(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster) {
    return multiMap(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster, Executor executor) {
    return multiMap(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, LogConfig config) {
    return multiMap(name, cluster, config, executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, LogConfig config, Executor executor) {
    return multiMap(name, cluster, config, executor);
  }

  @Override
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster, LogConfig config) {
    return multiMap(name, cluster, config, executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster, LogConfig config, Executor executor) {
    return stateMachine(name, MultiMapState.class, new DefaultMultiMapState<>(), cluster, config, executor).thenApplyAsync(stateMachine -> new DefaultAsyncMultiMap(stateMachine), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name) {
    return list(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, Executor executor) {
    return list(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster) {
    return list(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster, Executor executor) {
    return list(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, LogConfig config) {
    return list(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, LogConfig config, Executor executor) {
    return list(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster, LogConfig config) {
    return list(name, cluster, config, executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster, LogConfig config, Executor executor) {
    return stateMachine(name, ListState.class, new DefaultListState<>(), cluster, config, executor).thenApplyAsync(stateMachine -> new DefaultAsyncList(stateMachine), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name) {
    return set(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, Executor executor) {
    return set(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster) {
    return set(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster, Executor executor) {
    return set(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, LogConfig config) {
    return set(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, LogConfig config, Executor executor) {
    return set(name, cluster, config, executor);
  }

  @Override
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster, LogConfig config) {
    return set(name, cluster, config, executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster, LogConfig config, Executor executor) {
    return stateMachine(name, SetState.class, new DefaultSetState<>(), cluster, config, executor).thenApplyAsync(stateMachine -> new DefaultAsyncSet(stateMachine), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name) {
    return lock(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, Executor executor) {
    return lock(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster) {
    return lock(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster, Executor executor) {
    return lock(name, cluster, new LogConfig(), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, LogConfig config) {
    return lock(name, cluster, config, executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, LogConfig config, Executor executor) {
    return lock(name, cluster, config, executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster, LogConfig config) {
    return lock(name, cluster, config, executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster, LogConfig config, Executor executor) {
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
