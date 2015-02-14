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
import net.kuujo.copycat.atomic.internal.DefaultAsyncBoolean;
import net.kuujo.copycat.atomic.internal.DefaultAsyncLong;
import net.kuujo.copycat.atomic.internal.DefaultAsyncReference;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.ManagedCluster;
import net.kuujo.copycat.collections.*;
import net.kuujo.copycat.collections.internal.collection.DefaultAsyncList;
import net.kuujo.copycat.collections.internal.collection.DefaultAsyncSet;
import net.kuujo.copycat.collections.internal.map.DefaultAsyncMap;
import net.kuujo.copycat.collections.internal.map.DefaultAsyncMultiMap;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.election.LeaderElectionConfig;
import net.kuujo.copycat.election.internal.DefaultLeaderElection;
import net.kuujo.copycat.event.EventLog;
import net.kuujo.copycat.event.EventLogConfig;
import net.kuujo.copycat.event.internal.DefaultEventLog;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.raft.RaftConfig;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.resource.ResourceConfig;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.state.StateLog;
import net.kuujo.copycat.state.StateLogConfig;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.StateMachineConfig;
import net.kuujo.copycat.state.internal.DefaultStateLog;
import net.kuujo.copycat.state.internal.DefaultStateMachine;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.util.internal.Hash;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * Internal Copycat implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycat implements Copycat {
  private final ProtocolServerRegistry registry;
  private final CopycatConfig config;
  private final ManagedCluster cluster;
  private final RaftContext context;
  @SuppressWarnings("rawtypes")
  private final Map<String, Resource> resources = new ConcurrentHashMap<>(1024);

  public DefaultCopycat(CopycatConfig config) {
    this.registry = new ProtocolServerRegistry(config.getClusterConfig().getProtocol());
    this.config = config;
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(config.getName()));
    ClusterConfig cluster = createClusterConfig("", executor);
    this.context = new RaftContext(config.getName(), cluster.getLocalMember(), new RaftConfig(cluster.toMap()).withReplicas(cluster.getMembers()).withLog(new BufferedLog()), executor);
    this.cluster = new ManagedCluster(cluster.getProtocol(), context, config.getDefaultSerializer(), config.getDefaultExecutor() != null ? config.getDefaultExecutor() : Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(config.getName() + "-%d")));
  }

  @Override
  public Cluster cluster() {
    return cluster;
  }

  @Override
  public CopycatConfig config() {
    return config;
  }

  /**
   * Sets default configuration options on the given configuration.
   */
  private <T extends ResourceConfig<T>> T setDefaults(T config) {
    if (config.getSerializer() == null) {
      config.setSerializer(this.config.getDefaultSerializer());
    }
    if (config.getExecutor() == null) {
      config.setExecutor(this.config.getDefaultExecutor());
    }
    return config;
  }

  /**
   * Creates an executor for the given resource.
   */
  private ScheduledExecutorService createExecutor(String name) {
    return Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-" + name + "-%d"));
  }

  /**
   * Creates a cluster configuration for the given resource.
   */
  private ClusterConfig createClusterConfig(String name, ScheduledExecutorService executor) {
    return this.config.getClusterConfig().withProtocol(new CoordinatedProtocol(Hash.hash32(name.getBytes()), this.config.getClusterConfig().getProtocol(), registry, executor));
  }

  /**
   * Creates a new resource.
   */
  @SuppressWarnings("unchecked")
  private <T extends Resource<T>, U extends ResourceConfig<U>> T createResource(String name, U config, Function<ResourceContext, T> factory) {
    return (T) resources.computeIfAbsent(name, n -> {
      ScheduledExecutorService executor = createExecutor(n);
      return factory.apply(new ResourceContext(n, setDefaults(config), createClusterConfig(name, executor), executor));
    });
  }

  @Override
  public <T> EventLog<T> createEventLog(String name) {
    return createEventLog(name, new EventLogConfig(name));
  }

  @Override
  public <T> EventLog<T> createEventLog(String name, EventLogConfig config) {
    return createResource(name, config, DefaultEventLog::new);
  }

  @Override
  public <T> StateLog<T> createStateLog(String name) {
    return createStateLog(name, new StateLogConfig(name));
  }

  @Override
  public <T> StateLog<T> createStateLog(String name, StateLogConfig config) {
    return createResource(name, config, DefaultStateLog::new);
  }

  @Override
  public <T> StateMachine<T> createStateMachine(String name, Class<T> stateType, Class<? extends T> initialState) {
    return createStateMachine(name, new StateMachineConfig().withStateType(stateType).withInitialState(initialState));
  }

  @Override
  public <T> StateMachine<T> createStateMachine(String name, StateMachineConfig config) {
    return createResource(name, config, DefaultStateMachine::new);
  }

  @Override
  public LeaderElection createLeaderElection(String name) {
    return createLeaderElection(name, new LeaderElectionConfig(name));
  }

  @Override
  public LeaderElection createLeaderElection(String name, LeaderElectionConfig config) {
    return createResource(name, config, DefaultLeaderElection::new);
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(String name) {
    return createMap(name, new AsyncMapConfig(name));
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(String name, AsyncMapConfig config) {
    return createResource(name, config, DefaultAsyncMap::new);
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(String name) {
    return createMultiMap(name, new AsyncMultiMapConfig(name));
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(String name, AsyncMultiMapConfig config) {
    return createResource(name, config, DefaultAsyncMultiMap::new);
  }

  @Override
  public <T> AsyncList<T> createList(String name) {
    return createList(name, new AsyncListConfig(name));
  }

  @Override
  public <T> AsyncList<T> createList(String name, AsyncListConfig config) {
    return createResource(name, config, DefaultAsyncList::new);
  }

  @Override
  public <T> AsyncSet<T> createSet(String name) {
    return createSet(name, new AsyncSetConfig(name));
  }

  @Override
  public <T> AsyncSet<T> createSet(String name, AsyncSetConfig config) {
    return createResource(name, config, DefaultAsyncSet::new);
  }

  @Override
  public AsyncLong createLong(String name) {
    return createLong(name, new AsyncLongConfig());
  }

  @Override
  public AsyncLong createLong(String name, AsyncLongConfig config) {
    return createResource(name, config, DefaultAsyncLong::new);
  }

  @Override
  public AsyncBoolean createBoolean(String name) {
    return createBoolean(name, new AsyncBooleanConfig());
  }

  @Override
  public AsyncBoolean createBoolean(String name, AsyncBooleanConfig config) {
    return createResource(name, config, DefaultAsyncBoolean::new);
  }

  @Override
  public <T> AsyncReference<T> createReference(String name) {
    return createReference(name, new AsyncReferenceConfig());
  }

  @Override
  public <T> AsyncReference<T> createReference(String name, AsyncReferenceConfig config) {
    return createResource(name, config, DefaultAsyncReference::new);
  }

  @Override
  public CompletableFuture<Copycat> open() {
    return cluster.open().thenCompose(v -> context.open()).thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return cluster.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return context.close().thenCompose(v -> cluster.close());
  }

  @Override
  public boolean isClosed() {
    return context.isClosed();
  }

  @Override
  public String toString() {
    return String.format("%s[cluster=%s]", getClass().getSimpleName(), cluster());
  }

}
