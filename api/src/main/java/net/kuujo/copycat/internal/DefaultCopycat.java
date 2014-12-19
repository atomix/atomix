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
  private final CopycatConfig config;
  private final ExecutionContext executor;

  public DefaultCopycat(ClusterConfig cluster, CopycatConfig config, ExecutionContext executor) {
    this.coordinator = new DefaultClusterCoordinator(cluster, ExecutionContext.create());
    this.config = config;
    this.executor = executor;
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name) {
    return eventLog(name, new EventLogConfig().withSerializer(config.getSerializer()));
  }

  @Override
  public synchronized <T> CompletableFuture<EventLog<T>> eventLog(String name, EventLogConfig config) {
    return coordinator.createResource(name).thenApplyAsync(context -> new DefaultEventLog<T>(name, context, coordinator, config, executor), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name) {
    return stateLog(name, new StateLogConfig().withSerializer(config.getSerializer()));
  }

  @Override
  public synchronized <T> CompletableFuture<StateLog<T>> stateLog(String name, StateLogConfig config) {
    return coordinator.createResource(name).thenApplyAsync(context -> new DefaultStateLog<T>(name, context, coordinator, config, executor), executor);
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState) {
    return stateMachine(name, stateType, initialState, new StateMachineConfig());
  }

  @Override
  public synchronized <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, StateMachineConfig config) {
    return this.<List<Object>>stateLog(name, config).thenApplyAsync(log -> new DefaultStateMachine<T>(stateType, initialState, log), executor);
  }

  @Override
  public synchronized CompletableFuture<LeaderElection> election(String name) {
    return coordinator.createResource(name).thenApplyAsync(context -> new DefaultLeaderElection(name, context, coordinator, executor), executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> getMap(String name) {
    return stateMachine(name, AsyncMapState.class, new DefaultAsyncMapState<>()).thenApplyAsync(stateMachine -> new DefaultAsyncMap(stateMachine), executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> CompletableFuture<AsyncMap<K, V>> getMap(String name, AsyncMapConfig config) {
    return stateMachine(name, AsyncMapState.class, new DefaultAsyncMapState<>(), config).thenApplyAsync(stateMachine -> new DefaultAsyncMap(stateMachine), executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> getMultiMap(String name) {
    return stateMachine(name, AsyncMultiMapState.class, new DefaultAsyncMultiMapState<>()).thenApplyAsync(stateMachine -> new DefaultAsyncMultiMap(stateMachine), executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> CompletableFuture<AsyncMultiMap<K, V>> getMultiMap(String name, AsyncMultiMapConfig config) {
    return stateMachine(name, AsyncMultiMapState.class, new DefaultAsyncMultiMapState<>(), config).thenApplyAsync(stateMachine -> new DefaultAsyncMultiMap(stateMachine), executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> CompletableFuture<AsyncList<T>> getList(String name) {
    return stateMachine(name, AsyncListState.class, new DefaultAsyncListState<>()).thenApplyAsync(stateMachine -> new DefaultAsyncList(stateMachine), executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> CompletableFuture<AsyncList<T>> getList(String name, AsyncListConfig config) {
    return stateMachine(name, AsyncListState.class, new DefaultAsyncListState<>(), config).thenApplyAsync(stateMachine -> new DefaultAsyncList(stateMachine), executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> CompletableFuture<AsyncSet<T>> getSet(String name) {
    return stateMachine(name, AsyncSetState.class, new DefaultAsyncSetState<>()).thenApplyAsync(stateMachine -> new DefaultAsyncSet(stateMachine), executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> CompletableFuture<AsyncSet<T>> getSet(String name, AsyncSetConfig config) {
    return stateMachine(name, AsyncSetState.class, new DefaultAsyncSetState<>(), config).thenApplyAsync(stateMachine -> new DefaultAsyncSet(stateMachine), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> getLock(String name) {
    return stateMachine(name, AsyncLockState.class, new UnlockedAsyncLockState()).thenApplyAsync(stateMachine -> new DefaultAsyncLock(stateMachine), executor);
  }

  @Override
  public synchronized CompletableFuture<AsyncLock> getLock(String name, AsyncLockConfig config) {
    return stateMachine(name, AsyncLockState.class, new UnlockedAsyncLockState(), config).thenApplyAsync(stateMachine -> new DefaultAsyncLock(stateMachine), executor);
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
