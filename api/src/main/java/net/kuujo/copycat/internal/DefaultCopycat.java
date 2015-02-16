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
import net.kuujo.copycat.protocol.Protocol;
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
  private final Protocol protocol;
  private final ResourceContext context;
  @SuppressWarnings("rawtypes")
  private final Map<String, Resource> resources = new ConcurrentHashMap<>(1024);

  public DefaultCopycat(CopycatConfig config) {
    this(config, Executors.newSingleThreadExecutor(new NamedThreadFactory(config.getName())));
  }

  public DefaultCopycat(CopycatConfig config, Executor executor) {
    this.protocol = config.getClusterConfig().getProtocol();
    this.registry = new ProtocolServerRegistry(protocol);
    this.config = config;
    this.context = new ResourceContext(new CopycatResourceConfig(config.toMap()).withLog(new BufferedLog()),
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
    return this.config.getClusterConfig().withProtocol(new CoordinatedProtocol(Hash.hash32(name.getBytes()), protocol, registry, executor));
  }

  /**
   * Creates a new resource.
   */
  @SuppressWarnings("unchecked")
  private <T extends Resource<T>, U extends ResourceConfig<U>> T createResource(U config, Executor executor, Function<ResourceContext, T> factory) {
    return (T) resources.computeIfAbsent(config.getName(), n -> {
      ScheduledExecutorService scheduler = createExecutor(n);
      return factory.apply(new ResourceContext(setDefaults(config), createClusterConfig(config.getName(), scheduler), scheduler, executor));
    });
  }

  @Override
  public <T> EventLog<T> createEventLog(String name) {
    return createResource(new EventLogConfig(name).withDefaultName(name), context.executor(), DefaultEventLog::new);
  }

  @Override
  public <T> EventLog<T> createEventLog(String name, Executor executor) {
    return createResource(new EventLogConfig(name).withDefaultName(name), executor, DefaultEventLog::new);
  }

  @Override
  public <T> EventLog<T> createEventLog(EventLogConfig config) {
    return createResource(config, context.executor(), DefaultEventLog::new);
  }

  @Override
  public <T> EventLog<T> createEventLog(EventLogConfig config, Executor executor) {
    return createResource(config, executor, DefaultEventLog::new);
  }

  @Override
  public <T> StateLog<T> createStateLog(String name) {
    return createResource(new StateLogConfig(name).withDefaultName(name), context.executor(), DefaultStateLog::new);
  }

  @Override
  public <T> StateLog<T> createStateLog(String name, Executor executor) {
    return createResource(new StateLogConfig(name).withDefaultName(name), context.executor(), DefaultStateLog::new);
  }

  @Override
  public <T> StateLog<T> createStateLog(StateLogConfig config) {
    return createResource(config, context.executor(), DefaultStateLog::new);
  }

  @Override
  public <T> StateLog<T> createStateLog(StateLogConfig config, Executor executor) {
    return createResource(config, executor, DefaultStateLog::new);
  }

  @Override
  public <T> StateMachine<T> createStateMachine(String name, Class<T> stateType, Class<? extends T> initialState) {
    return createResource(new StateMachineConfig(name).withStateType(stateType).withInitialState(initialState).withDefaultName(name), context.executor(), DefaultStateMachine::new);
  }

  @Override
  public <T> StateMachine<T> createStateMachine(String name, Class<T> stateType, Class<? extends T> initialState, Executor executor) {
    return createResource(new StateMachineConfig(name).withStateType(stateType).withInitialState(initialState).withDefaultName(name), executor, DefaultStateMachine::new);
  }

  @Override
  public <T> StateMachine<T> createStateMachine(StateMachineConfig config) {
    return createResource(config, context.executor(), DefaultStateMachine::new);
  }

  @Override
  public <T> StateMachine<T> createStateMachine(StateMachineConfig config, Executor executor) {
    return createResource(config, executor, DefaultStateMachine::new);
  }

  @Override
  public LeaderElection createLeaderElection(String name) {
    return createResource(new LeaderElectionConfig(name).withDefaultName(name), context.executor(), DefaultLeaderElection::new);
  }

  @Override
  public LeaderElection createLeaderElection(String name, Executor executor) {
    return createResource(new LeaderElectionConfig(name).withDefaultName(name), executor, DefaultLeaderElection::new);
  }

  @Override
  public LeaderElection createLeaderElection(LeaderElectionConfig config) {
    return createResource(config, context.executor(), DefaultLeaderElection::new);
  }

  @Override
  public LeaderElection createLeaderElection(LeaderElectionConfig config, Executor executor) {
    return createResource(config, executor, DefaultLeaderElection::new);
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(String name) {
    return createResource(new AsyncMapConfig(name).withDefaultName(name), context.executor(), DefaultAsyncMap::new);
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(String name, Executor executor) {
    return createResource(new AsyncMapConfig(name).withDefaultName(name), executor, DefaultAsyncMap::new);
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(AsyncMapConfig config) {
    return createResource(config, context.executor(), DefaultAsyncMap::new);
  }

  @Override
  public <K, V> AsyncMap<K, V> createMap(AsyncMapConfig config, Executor executor) {
    return createResource(config, executor, DefaultAsyncMap::new);
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(String name) {
    return createResource(new AsyncMultiMapConfig(name).withDefaultName(name), context.executor(), DefaultAsyncMultiMap::new);
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(String name, Executor executor) {
    return createResource(new AsyncMultiMapConfig(name).withDefaultName(name), executor, DefaultAsyncMultiMap::new);
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(AsyncMultiMapConfig config) {
    return createResource(config, context.executor(), DefaultAsyncMultiMap::new);
  }

  @Override
  public <K, V> AsyncMultiMap<K, V> createMultiMap(AsyncMultiMapConfig config, Executor executor) {
    return createResource(config, executor, DefaultAsyncMultiMap::new);
  }

  @Override
  public <T> AsyncList<T> createList(String name) {
    return createResource(new AsyncListConfig(name).withDefaultName(name), context.executor(), DefaultAsyncList::new);
  }

  @Override
  public <T> AsyncList<T> createList(String name, Executor executor) {
    return createResource(new AsyncListConfig(name).withDefaultName(name), executor, DefaultAsyncList::new);
  }

  @Override
  public <T> AsyncList<T> createList(AsyncListConfig config) {
    return createResource(config, context.executor(), DefaultAsyncList::new);
  }

  @Override
  public <T> AsyncList<T> createList(AsyncListConfig config, Executor executor) {
    return createResource(config, executor, DefaultAsyncList::new);
  }

  @Override
  public <T> AsyncSet<T> createSet(String name) {
    return createResource(new AsyncSetConfig(name).withDefaultName(name), context.executor(), DefaultAsyncSet::new);
  }

  @Override
  public <T> AsyncSet<T> createSet(String name, Executor executor) {
    return createResource(new AsyncSetConfig(name).withDefaultName(name), executor, DefaultAsyncSet::new);
  }

  @Override
  public <T> AsyncSet<T> createSet(AsyncSetConfig config) {
    return createResource(config, context.executor(), DefaultAsyncSet::new);
  }

  @Override
  public <T> AsyncSet<T> createSet(AsyncSetConfig config, Executor executor) {
    return createResource(config, executor, DefaultAsyncSet::new);
  }

  @Override
  public AsyncLong createLong(String name) {
    return createResource(new AsyncLongConfig(name).withDefaultName(name), context.executor(), DefaultAsyncLong::new);
  }

  @Override
  public AsyncLong createLong(String name, Executor executor) {
    return createResource(new AsyncLongConfig(name).withDefaultName(name), executor, DefaultAsyncLong::new);
  }

  @Override
  public AsyncLong createLong(AsyncLongConfig config) {
    return createResource(config, context.executor(), DefaultAsyncLong::new);
  }

  @Override
  public AsyncLong createLong(AsyncLongConfig config, Executor executor) {
    return createResource(config, executor, DefaultAsyncLong::new);
  }

  @Override
  public AsyncBoolean createBoolean(String name) {
    return createResource(new AsyncBooleanConfig(name).withDefaultName(name), context.executor(), DefaultAsyncBoolean::new);
  }

  @Override
  public AsyncBoolean createBoolean(String name, Executor executor) {
    return createResource(new AsyncBooleanConfig(name).withDefaultName(name), executor, DefaultAsyncBoolean::new);
  }

  @Override
  public AsyncBoolean createBoolean(AsyncBooleanConfig config) {
    return createResource(config, context.executor(), DefaultAsyncBoolean::new);
  }

  @Override
  public AsyncBoolean createBoolean(AsyncBooleanConfig config, Executor executor) {
    return createResource(config, executor, DefaultAsyncBoolean::new);
  }

  @Override
  public <T> AsyncReference<T> createReference(String name) {
    return createResource(new AsyncReferenceConfig(name).withDefaultName(name), context.executor(), DefaultAsyncReference::new);
  }

  @Override
  public <T> AsyncReference<T> createReference(String name, Executor executor) {
    return createResource(new AsyncReferenceConfig(name).withDefaultName(name), executor, DefaultAsyncReference::new);
  }

  @Override
  public <T> AsyncReference<T> createReference(AsyncReferenceConfig config) {
    return createResource(config, context.executor(), DefaultAsyncReference::new);
  }

  @Override
  public <T> AsyncReference<T> createReference(AsyncReferenceConfig config, Executor executor) {
    return createResource(config, executor, DefaultAsyncReference::new);
  }

  @Override
  public CompletableFuture<Copycat> open() {
    return context.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return context.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return context.close();
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
