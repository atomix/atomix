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
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.*;
import net.kuujo.copycat.collections.internal.collection.*;
import net.kuujo.copycat.collections.internal.lock.AsyncLockState;
import net.kuujo.copycat.collections.internal.lock.DefaultAsyncLock;
import net.kuujo.copycat.collections.internal.lock.UnlockedAsyncLockState;
import net.kuujo.copycat.collections.internal.map.*;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.election.internal.DefaultLeaderElection;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;

/**
 * Internal Copycat implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycat implements Copycat {
  private final CopycatCoordinator coordinator;
  private boolean open;

  public DefaultCopycat(ClusterConfig cluster, CopycatConfig config, ExecutionContext context) {
    this.coordinator = new DefaultCopycatCoordinator(cluster, new BufferedLog("copycat", config), context);
  }

  @Override
  public Cluster cluster() {
    return coordinator.cluster();
  }

  @Override
  public EventLog eventLog(String name) {
    return eventLog(name, new EventLogConfig());
  }

  @Override
  public EventLog eventLog(String name, EventLogConfig config) {
    return new DefaultEventLog(name, coordinator, config);
  }

  @Override
  public StateLog stateLog(String name) {
    return stateLog(name, new StateLogConfig());
  }

  @Override
  public StateLog stateLog(String name, StateLogConfig config) {
    return new DefaultStateLog(name, coordinator, config);
  }

  @Override
  public <T> StateMachine<T> stateMachine(String name, Class<T> stateType, T state) {
    return stateMachine(name, stateType, state, new StateMachineConfig());
  }

  @Override
  public <T> StateMachine<T> stateMachine(String name, Class<T> stateType, T state, StateMachineConfig config) {
    return new DefaultStateMachine<>(stateType, state, stateLog(name, config));
  }

  @Override
  public LeaderElection election(String name) {
    return new DefaultLeaderElection(name, coordinator);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> AsyncMap<K, V> getMap(String name) {
    return new DefaultAsyncMap(stateMachine(name, AsyncMapState.class, new DefaultAsyncMapState<>()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> AsyncMap<K, V> getMap(String name, AsyncMapConfig config) {
    return new DefaultAsyncMap(stateMachine(name, AsyncMapState.class, new DefaultAsyncMapState<>(), config));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> AsyncMultiMap<K, V> getMultiMap(String name) {
    return new DefaultAsyncMultiMap(stateMachine(name, AsyncMultiMapState.class, new DefaultAsyncMultiMapState<>()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> AsyncMultiMap<K, V> getMultiMap(String name, AsyncMultiMapConfig config) {
    return new DefaultAsyncMultiMap(stateMachine(name, AsyncMultiMapState.class, new DefaultAsyncMultiMapState<>(), config));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> AsyncList<T> getList(String name) {
    return new DefaultAsyncList(stateMachine(name, AsyncListState.class, new DefaultAsyncListState<>()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> AsyncList<T> getList(String name, AsyncListConfig config) {
    return new DefaultAsyncList(stateMachine(name, AsyncListState.class, new DefaultAsyncListState<>(), config));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> AsyncSet<T> getSet(String name) {
    return new DefaultAsyncSet(stateMachine(name, AsyncSetState.class, new DefaultAsyncSetState<>()));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> AsyncSet<T> getSet(String name, AsyncSetConfig config) {
    return new DefaultAsyncSet(stateMachine(name, AsyncSetState.class, new DefaultAsyncSetState<>(), config));
  }

  @Override
  public AsyncLock getLock(String name) {
    return new DefaultAsyncLock(stateMachine(name, AsyncLockState.class, new UnlockedAsyncLockState()));
  }

  @Override
  public AsyncLock getLock(String name, AsyncLockConfig config) {
    return new DefaultAsyncLock(stateMachine(name, AsyncLockState.class, new UnlockedAsyncLockState(), config));
  }

  @Override
  public CompletableFuture<Void> open() {
    return coordinator.open();
  }

  @Override
  public CompletableFuture<Void> close() {
    return coordinator.close();
  }

}
