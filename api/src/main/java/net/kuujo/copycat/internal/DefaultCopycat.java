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
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;

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
  private final CopycatConfig config;
  private final Executor executor;

  public DefaultCopycat(CopycatConfig config) {
    this.coordinator = new DefaultClusterCoordinator(config.resolve());
    this.config = config;
    this.executor = config.getDefaultExecutor() != null ? config.getDefaultExecutor() : Executors.newSingleThreadExecutor(new NamedThreadFactory(config.getName() + "-%d"));
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
  public <T> EventLog<T> createEventLog(String name) {
    return createEventLog(name, new EventLogConfig(name));
  }

  @Override
  public <T> EventLog<T> createEventLog(String name, EventLogConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> StateLog<T> createStateLog(String name) {
    return createStateLog(name, new StateLogConfig(name));
  }

  @Override
  public <T> StateLog<T> createStateLog(String name, StateLogConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> StateMachine<T> createStateMachine(String name, Class<T> stateType, Class<? extends T> initialState) {
    return createStateMachine(name, new StateMachineConfig().withStateType(stateType).withInitialState(initialState));
  }

  @Override
  public <T> StateMachine<T> createStateMachine(String name, StateMachineConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public LeaderElection createLeaderElection(String name) {
    return createLeaderElection(name, new LeaderElectionConfig(name));
  }

  @Override
  public LeaderElection createLeaderElection(String name, LeaderElectionConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(String name) {
    return createMap(name, new AsyncMapConfig(name));
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(String name, AsyncMapConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(String name) {
    return createMultiMap(name, new AsyncMultiMapConfig(name));
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(String name, AsyncMultiMapConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> AsyncList<T> createList(String name) {
    return createList(name, new AsyncListConfig(name));
  }

  @Override
  public <T> AsyncList<T> createList(String name, AsyncListConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> AsyncSet<T> createSet(String name) {
    return createSet(name, new AsyncSetConfig(name));
  }

  @Override
  public <T> AsyncSet<T> createSet(String name, AsyncSetConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public AsyncLong createLong(String name) {
    return createLong(name, new AsyncLongConfig());
  }

  @Override
  public AsyncLong createLong(String name, AsyncLongConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public AsyncBoolean createBoolean(String name) {
    return createBoolean(name, new AsyncBooleanConfig());
  }

  @Override
  public AsyncBoolean createBoolean(String name, AsyncBooleanConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public <T> AsyncReference<T> createReference(String name) {
    return createReference(name, new AsyncReferenceConfig());
  }

  @Override
  public <T> AsyncReference<T> createReference(String name, AsyncReferenceConfig config) {
    return coordinator.getResource(name, config.resolve(this.config.getClusterConfig())
      .withDefaultSerializer(this.config.getDefaultSerializer().copy())
      .withDefaultExecutor(this.config.getDefaultExecutor()));
  }

  @Override
  public CompletableFuture<Copycat> open() {
    return coordinator.open().thenApplyAsync(v -> this, executor);
  }

  @Override
  public boolean isOpen() {
    return coordinator.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return coordinator.close().thenRunAsync(() -> {}, executor);
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
