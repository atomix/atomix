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
import net.kuujo.copycat.atomic.*;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.internal.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.internal.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.collections.*;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.election.LeaderElectionConfig;
import net.kuujo.copycat.event.EventLog;
import net.kuujo.copycat.event.EventLogConfig;
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

  public DefaultCopycat(CopycatConfig config) {
    this.coordinator = new DefaultClusterCoordinator(config.resolve());
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
    return eventLog(name, new EventLogConfig(name));
  }

  @Override
  public <T> EventLog<T> eventLog(String name, EventLogConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> StateLog<T> stateLog(String name) {
    return stateLog(name, new StateLogConfig(name));
  }

  @Override
  public <T> StateLog<T> stateLog(String name, StateLogConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> StateMachine<T> stateMachine(String name, Class<T> stateType, Class<? extends T> initialState) {
    return stateMachine(name, new StateMachineConfig().withStateType(stateType).withInitialState(initialState));
  }

  @Override
  public <T> StateMachine<T> stateMachine(String name, StateMachineConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public LeaderElection leaderElection(String name) {
    return leaderElection(name, new LeaderElectionConfig(name));
  }

  @Override
  public LeaderElection leaderElection(String name, LeaderElectionConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <K, V> AsyncMap<K, V> map(String name) {
    return map(name, new AsyncMapConfig(name));
  }

  @Override
  public <K, V> AsyncMap<K, V> map(String name, AsyncMapConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> multiMap(String name) {
    return multiMap(name, new AsyncMultiMapConfig(name));
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> multiMap(String name, AsyncMultiMapConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> AsyncList<T> list(String name) {
    return list(name, new AsyncListConfig(name));
  }

  @Override
  public <T> AsyncList<T> list(String name, AsyncListConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> AsyncSet<T> set(String name) {
    return set(name, new AsyncSetConfig(name));
  }

  @Override
  public <T> AsyncSet<T> set(String name, AsyncSetConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public AsyncLock lock(String name) {
    return lock(name, new AsyncLockConfig(name));
  }

  @Override
  public AsyncLock lock(String name, AsyncLockConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public AsyncAtomicLong atomicLong(String name) {
    return atomicLong(name, new AsyncAtomicLongConfig());
  }

  @Override
  public AsyncAtomicLong atomicLong(String name, AsyncAtomicLongConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public AsyncAtomicBoolean atomicBoolean(String name) {
    return atomicBoolean(name, new AsyncAtomicBooleanConfig());
  }

  @Override
  public AsyncAtomicBoolean atomicBoolean(String name, AsyncAtomicBooleanConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> AsyncAtomicReference<T> atomicReference(String name) {
    return atomicReference(name, new AsyncAtomicReferenceConfig());
  }

  @Override
  public <T> AsyncAtomicReference<T> atomicReference(String name, AsyncAtomicReferenceConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public CompletableFuture<Copycat> open() {
    return coordinator.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return coordinator.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return coordinator.close();
  }

  @Override
  public boolean isClosed() {
    return coordinator.isClosed();
  }

  @Override
  public String toString() {
    return String.format("%s[cluster=%s]", getClass().getSimpleName(), cluster());
  }

}
