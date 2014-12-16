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
  private boolean open;

  public DefaultCopycat(ClusterConfig cluster, CopycatConfig config, ExecutionContext executor) {
    this.coordinator = new DefaultClusterCoordinator(cluster, ExecutionContext.create());
    this.config = config;
    this.executor = executor;
  }

  @Override
  public synchronized <T> EventLog<T> eventLog(String name) {
    return eventLog(name, new EventLogConfig().withSerializer(config.getSerializer()));
  }

  @Override
  public synchronized <T> EventLog<T> eventLog(String name, EventLogConfig config) {
    return new DefaultEventLog<T>(name, coordinator.getResource(name), config, executor);
  }

  @Override
  public synchronized <T> StateLog<T> stateLog(String name) {
    return stateLog(name, new StateLogConfig().withSerializer(config.getSerializer()));
  }

  @Override
  public synchronized <T> StateLog<T> stateLog(String name, StateLogConfig config) {
    return new DefaultStateLog<T>(name, coordinator.getResource(name), config, executor);
  }

  @Override
  public synchronized <T> StateMachine<T> stateMachine(String name, Class<T> stateType, T initialState) {
    return stateMachine(name, stateType, initialState, new StateMachineConfig());
  }

  @Override
  public synchronized <T> StateMachine<T> stateMachine(String name, Class<T> stateType, T initialState, StateMachineConfig config) {
    return new DefaultStateMachine<>(stateType, initialState, stateLog(name, config));
  }

  @Override
  public synchronized LeaderElection election(String name) {
    return new DefaultLeaderElection(name, coordinator.getResource(name), executor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> AsyncMap<K, V> getMap(String name) {
    return new DefaultAsyncMap(stateMachine(name, AsyncMapState.class, new DefaultAsyncMapState<>()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> AsyncMap<K, V> getMap(String name, AsyncMapConfig config) {
    return new DefaultAsyncMap(stateMachine(name, AsyncMapState.class, new DefaultAsyncMapState<>(), config));
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> AsyncMultiMap<K, V> getMultiMap(String name) {
    return new DefaultAsyncMultiMap(stateMachine(name, AsyncMultiMapState.class, new DefaultAsyncMultiMapState<>()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <K, V> AsyncMultiMap<K, V> getMultiMap(String name, AsyncMultiMapConfig config) {
    return new DefaultAsyncMultiMap(stateMachine(name, AsyncMultiMapState.class, new DefaultAsyncMultiMapState<>(), config));
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> AsyncList<T> getList(String name) {
    return new DefaultAsyncList(stateMachine(name, AsyncListState.class, new DefaultAsyncListState<>()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> AsyncList<T> getList(String name, AsyncListConfig config) {
    return new DefaultAsyncList(stateMachine(name, AsyncListState.class, new DefaultAsyncListState<>(), config));
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> AsyncSet<T> getSet(String name) {
    return new DefaultAsyncSet(stateMachine(name, AsyncSetState.class, new DefaultAsyncSetState<>()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> AsyncSet<T> getSet(String name, AsyncSetConfig config) {
    return new DefaultAsyncSet(stateMachine(name, AsyncSetState.class, new DefaultAsyncSetState<>(), config));
  }

  @Override
  public synchronized AsyncLock getLock(String name) {
    return new DefaultAsyncLock(stateMachine(name, AsyncLockState.class, new UnlockedAsyncLockState()));
  }

  @Override
  public synchronized AsyncLock getLock(String name, AsyncLockConfig config) {
    return new DefaultAsyncLock(stateMachine(name, AsyncLockState.class, new UnlockedAsyncLockState(), config));
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    open = true;
    return coordinator.open();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    open = false;
    return coordinator.close();
  }

}
