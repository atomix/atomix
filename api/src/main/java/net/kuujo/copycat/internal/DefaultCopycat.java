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
import net.kuujo.copycat.CopycatConfig;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.collections.*;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.election.LeaderElectionConfig;
import net.kuujo.copycat.event.EventLog;
import net.kuujo.copycat.event.EventLogConfig;
import net.kuujo.copycat.internal.cluster.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.state.StateLog;
import net.kuujo.copycat.state.StateLogConfig;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.StateMachineConfig;

import java.util.concurrent.CompletableFuture;

/**
 * Internal Copycat implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycat implements Copycat {
  private final ClusterCoordinator coordinator;
  private final CopycatConfig config;

  public DefaultCopycat(String uri, CopycatConfig config) {
    this.coordinator = new DefaultClusterCoordinator(uri, config.resolve());
    this.config = config;
  }

  @Override
  public Cluster cluster() {
    return coordinator.cluster();
  }

  @Override
  public CopycatConfig config() {
    return config;
  }

  @Override
  public <T> EventLog<T> eventLog(String name) {
    return eventLog(name, new EventLogConfig());
  }

  @Override
  public <T> EventLog<T> eventLog(String name, EventLogConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> StateLog<T> stateLog(String name) {
    return stateLog(name, new StateLogConfig());
  }

  @Override
  public <T> StateLog<T> stateLog(String name, StateLogConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> StateMachine<T> stateMachine(String name, Class<T> stateType, Class<? extends T> initialState) {
    return stateMachine(name, new StateMachineConfig().withStateType(stateType).withInitialState(initialState));
  }

  @Override
  public <T> StateMachine<T> stateMachine(String name, StateMachineConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public LeaderElection leaderElection(String name) {
    return leaderElection(name, new LeaderElectionConfig());
  }

  @Override
  public LeaderElection leaderElection(String name, LeaderElectionConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <K, V> AsyncMap<K, V> map(String name) {
    return map(name, new AsyncMapConfig());
  }

  @Override
  public <K, V> AsyncMap<K, V> map(String name, AsyncMapConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> multiMap(String name) {
    return multiMap(name, new AsyncMultiMapConfig());
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> multiMap(String name, AsyncMultiMapConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> AsyncList<T> list(String name) {
    return list(name, new AsyncListConfig());
  }

  @Override
  public <T> AsyncList<T> list(String name, AsyncListConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> AsyncSet<T> set(String name) {
    return set(name, new AsyncSetConfig());
  }

  @Override
  public <T> AsyncSet<T> set(String name, AsyncSetConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public AsyncLock lock(String name) {
    return lock(name, new AsyncLockConfig());
  }

  @Override
  public AsyncLock lock(String name, AsyncLockConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public synchronized CompletableFuture<Copycat> open() {
    return coordinator.open().thenApply(v -> this);
  }

  @Override
  public synchronized boolean isOpen() {
    return coordinator.isOpen();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return coordinator.close();
  }

  @Override
  public synchronized boolean isClosed() {
    return coordinator.isClosed();
  }

}
