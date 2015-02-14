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
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Internal Copycat implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycat implements Copycat {
  private final ProtocolServerRegistry registry;
  private final CopycatConfig config;
  private final ResourceContext context;
  @SuppressWarnings("rawtypes")
  private final Map<String, Resource> resources = new ConcurrentHashMap<>(1024);

  public DefaultCopycat(CopycatConfig config) {
    this(config, Executors.newSingleThreadExecutor(new NamedThreadFactory(config.getName())));
  }

  public DefaultCopycat(CopycatConfig config, Executor executor) {
    this.registry = new ProtocolServerRegistry(config.getClusterConfig().getProtocol());
    this.config = config;
    this.context = new ResourceContext(config.getName(),
      new CopycatResourceConfig(config.toMap()).withLog(new BufferedLog()),
      config.getClusterConfig(),
      Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(config.getName())),
      executor);
  }

  @Override
  public Cluster cluster() {
    return context.cluster();
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
  private <T extends Resource<T>, U extends ResourceConfig<U>> T createResource(String name, U config, Executor executor, Function<ResourceContext, T> factory) {
    return (T) resources.computeIfAbsent(name, n -> {
      ScheduledExecutorService scheduler = createExecutor(n);
      return factory.apply(new ResourceContext(n, setDefaults(config), createClusterConfig(name, scheduler), scheduler, executor));
    });
  }

  @Override
  public <T> EventLog<T> createEventLog(String name) {
    return createResource(name, new EventLogConfig(name), context.executor(), DefaultEventLog::new);
  }

  @Override
  public <T> EventLog<T> createEventLog(String name, Executor executor) {
    return createResource(name, new EventLogConfig(name), executor, DefaultEventLog::new);
  }

  @Override
  public <T> EventLog<T> createEventLog(String name, EventLogConfig config) {
    return createResource(name, config, context.executor(), DefaultEventLog::new);
  }

  @Override
  public <T> EventLog<T> createEventLog(String name, EventLogConfig config, Executor executor) {
    return createResource(name, config, executor, DefaultEventLog::new);
  }

  @Override
  public <T> StateLog<T> createStateLog(String name) {
    return createResource(name, new StateLogConfig(name), context.executor(), DefaultStateLog::new);
  }

  @Override
  public <T> StateLog<T> createStateLog(String name, Executor executor) {
    return createResource(name, new StateLogConfig(name), context.executor(), DefaultStateLog::new);
  }

  @Override
  public <T> StateLog<T> createStateLog(String name, StateLogConfig config) {
    return createResource(name, config, context.executor(), DefaultStateLog::new);
  }

  @Override
  public <T> StateLog<T> createStateLog(String name, StateLogConfig config, Executor executor) {
    return createResource(name, config, executor, DefaultStateLog::new);
  }

  @Override
  public <T> StateMachine<T> createStateMachine(String name, Class<T> stateType, Class<? extends T> initialState) {
    return createResource(name, new StateMachineConfig().withStateType(stateType).withInitialState(initialState), context.executor(), DefaultStateMachine::new);
  }

  @Override
  public <T> StateMachine<T> createStateMachine(String name, Class<T> stateType, Class<? extends T> initialState, Executor executor) {
    return createResource(name, new StateMachineConfig().withStateType(stateType).withInitialState(initialState), executor, DefaultStateMachine::new);
  }

  @Override
  public <T> StateMachine<T> createStateMachine(String name, StateMachineConfig config) {
    return createResource(name, config, context.executor(), DefaultStateMachine::new);
  }

  @Override
  public <T> StateMachine<T> createStateMachine(String name, StateMachineConfig config, Executor executor) {
    return createResource(name, config, executor, DefaultStateMachine::new);
  }

  @Override
  public LeaderElection createLeaderElection(String name) {
    return createResource(name, new LeaderElectionConfig(name), context.executor(), DefaultLeaderElection::new);
  }

  @Override
  public LeaderElection createLeaderElection(String name, Executor executor) {
    return createResource(name, new LeaderElectionConfig(name), executor, DefaultLeaderElection::new);
  }

  @Override
  public LeaderElection createLeaderElection(String name, LeaderElectionConfig config) {
    return createResource(name, config, context.executor(), DefaultLeaderElection::new);
  }

  @Override
  public LeaderElection createLeaderElection(String name, LeaderElectionConfig config, Executor executor) {
    return createResource(name, config, executor, DefaultLeaderElection::new);
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(String name) {
    return createResource(name, new AsyncMapConfig(name), context.executor(), DefaultAsyncMap::new);
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(String name, Executor executor) {
    return createResource(name, new AsyncMapConfig(name), executor, DefaultAsyncMap::new);
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(String name, AsyncMapConfig config) {
    return createResource(name, config, context.executor(), DefaultAsyncMap::new);
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(String name, AsyncMapConfig config, Executor executor) {
    return createResource(name, config, executor, DefaultAsyncMap::new);
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(String name) {
    return createResource(name, new AsyncMultiMapConfig(name), context.executor(), DefaultAsyncMultiMap::new);
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(String name, Executor executor) {
    return createResource(name, new AsyncMultiMapConfig(name), executor, DefaultAsyncMultiMap::new);
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(String name, AsyncMultiMapConfig config) {
    return createResource(name, config, context.executor(), DefaultAsyncMultiMap::new);
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(String name, AsyncMultiMapConfig config, Executor executor) {
    return createResource(name, config, executor, DefaultAsyncMultiMap::new);
  }

  @Override
  public <T> AsyncList<T> createList(String name) {
    return createResource(name, new AsyncListConfig(name), context.executor(), DefaultAsyncList::new);
  }

  @Override
  public <T> AsyncList<T> createList(String name, Executor executor) {
    return createResource(name, new AsyncListConfig(name), executor, DefaultAsyncList::new);
  }

  @Override
  public <T> AsyncList<T> createList(String name, AsyncListConfig config) {
    return createResource(name, config, context.executor(), DefaultAsyncList::new);
  }

  @Override
  public <T> AsyncList<T> createList(String name, AsyncListConfig config, Executor executor) {
    return createResource(name, config, executor, DefaultAsyncList::new);
  }

  @Override
  public <T> AsyncSet<T> createSet(String name) {
    return createResource(name, new AsyncSetConfig(name), context.executor(), DefaultAsyncSet::new);
  }

  @Override
  public <T> AsyncSet<T> createSet(String name, Executor executor) {
    return createResource(name, new AsyncSetConfig(name), executor, DefaultAsyncSet::new);
  }

  @Override
  public <T> AsyncSet<T> createSet(String name, AsyncSetConfig config) {
    return createResource(name, config, context.executor(), DefaultAsyncSet::new);
  }

  @Override
  public <T> AsyncSet<T> createSet(String name, AsyncSetConfig config, Executor executor) {
    return createResource(name, config, executor, DefaultAsyncSet::new);
  }

  @Override
  public AsyncLong createLong(String name) {
    return createResource(name, new AsyncLongConfig(), context.executor(), DefaultAsyncLong::new);
  }

  @Override
  public AsyncLong createLong(String name, Executor executor) {
    return createResource(name, new AsyncLongConfig(), executor, DefaultAsyncLong::new);
  }

  @Override
  public AsyncLong createLong(String name, AsyncLongConfig config) {
    return createResource(name, config, context.executor(), DefaultAsyncLong::new);
  }

  @Override
  public AsyncLong createLong(String name, AsyncLongConfig config, Executor executor) {
    return createResource(name, config, executor, DefaultAsyncLong::new);
  }

  @Override
  public AsyncBoolean createBoolean(String name) {
    return createResource(name, new AsyncBooleanConfig(), context.executor(), DefaultAsyncBoolean::new);
  }

  @Override
  public AsyncBoolean createBoolean(String name, Executor executor) {
    return createResource(name, new AsyncBooleanConfig(), executor, DefaultAsyncBoolean::new);
  }

  @Override
  public AsyncBoolean createBoolean(String name, AsyncBooleanConfig config) {
    return createResource(name, config, context.executor(), DefaultAsyncBoolean::new);
  }

  @Override
  public AsyncBoolean createBoolean(String name, AsyncBooleanConfig config, Executor executor) {
    return createResource(name, config, executor, DefaultAsyncBoolean::new);
  }

  @Override
  public <T> AsyncReference<T> createReference(String name) {
    return createResource(name, new AsyncReferenceConfig(), context.executor(), DefaultAsyncReference::new);
  }

  @Override
  public <T> AsyncReference<T> createReference(String name, Executor executor) {
    return createResource(name, new AsyncReferenceConfig(), executor, DefaultAsyncReference::new);
  }

  @Override
  public <T> AsyncReference<T> createReference(String name, AsyncReferenceConfig config) {
    return createResource(name, config, context.executor(), DefaultAsyncReference::new);
  }

  @Override
  public <T> AsyncReference<T> createReference(String name, AsyncReferenceConfig config, Executor executor) {
    return createResource(name, config, executor, DefaultAsyncReference::new);
  }

  @Override
  public CompletableFuture<Copycat> open() {
    return context.open().thenCompose(v -> context.open()).thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return context.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return context.close().thenCompose(v -> context.close());
  }

  @Override
  public boolean isClosed() {
    return context.isClosed();
  }

  @Override
  public String toString() {
    return String.format("%s[cluster=%s]", getClass().getSimpleName(), cluster());
  }

  /**
   * Internal Copycat resource configuration.
   */
  private static class CopycatResourceConfig extends ResourceConfig<CopycatResourceConfig> {
    private CopycatResourceConfig() {
    }

    private CopycatResourceConfig(Map<String, Object> config, String... resources) {
      super(config, resources);
    }

    private CopycatResourceConfig(CopycatResourceConfig config) {
      super(config);
    }

    private CopycatResourceConfig(String... resources) {
      super(resources);
    }
  }

}
