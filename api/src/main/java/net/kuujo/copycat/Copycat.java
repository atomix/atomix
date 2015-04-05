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
package net.kuujo.copycat;

import net.kuujo.copycat.atomic.*;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.*;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.election.LeaderElectionConfig;
import net.kuujo.copycat.event.EventLog;
import net.kuujo.copycat.event.EventLogConfig;
import net.kuujo.copycat.internal.CoordinatedProtocol;
import net.kuujo.copycat.internal.ProtocolServerRegistry;
import net.kuujo.copycat.io.util.HashFunctions;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.resource.ResourceConfig;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.state.StateLog;
import net.kuujo.copycat.state.StateLogConfig;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.StateMachineConfig;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * Copycat.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Copycat implements Managed<Copycat> {

  /**
   * Creates a new Copycat instance with the default Copycat and cluster configuration.
   *
   * @return The Copycat instance.
   */
  public static Copycat create() {
    return create(new CopycatConfig().withClusterConfig(new ClusterConfig()));
  }

  /**
   * Creates a new Copycat instance with the default Copycat and cluster configuration.
   *
   * @param executor An executor on which to execute callbacks.
   * @return The Copycat instance.
   */
  public static Copycat create(Executor executor) {
    return create(new CopycatConfig().withClusterConfig(new ClusterConfig()), executor);
  }

  /**
   * Creates a new Copycat instance, overriding the default cluster configuration.
   *
   * @param cluster The global cluster configuration.
   * @return The Copycat instance.
   */
  public static Copycat create(ClusterConfig cluster) {
    return create(new CopycatConfig().withClusterConfig(cluster));
  }

  /**
   * Creates a new Copycat instance, overriding the default cluster configuration.
   *
   * @param cluster The global cluster configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The Copycat instance.
   */
  public static Copycat create(ClusterConfig cluster, Executor executor) {
    return create(new CopycatConfig().withClusterConfig(cluster), executor);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param config The global Copycat configuration.
   * @return The Copycat instance.
   */
  public static Copycat create(CopycatConfig config) {
    return new Copycat(config);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param config The global Copycat configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The Copycat instance.
   */
  public static Copycat create(CopycatConfig config, Executor executor) {
    return new Copycat(config, executor);
  }

  private final ProtocolServerRegistry registry;
  private final CopycatConfig config;
  private final Protocol protocol;
  private final ResourceContext context;
  @SuppressWarnings("rawtypes")
  private final Map<String, Resource> resources = new ConcurrentHashMap<>(1024);

  private Copycat(CopycatConfig config) {
    this(config, Executors.newSingleThreadExecutor(new NamedThreadFactory(config.getName())));
  }

  private Copycat(CopycatConfig config, Executor executor) {
    this.protocol = config.getClusterConfig().getProtocol();
    this.registry = new ProtocolServerRegistry(protocol);
    this.config = config;
    this.context = new ResourceContext(new ResourceConfig() {},
      config.getClusterConfig(),
      Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(config.getName())),
      executor);
  }

  /**
   * Returns the global Copycat configuration.
   *
   * @return The global Copycat configuration.
   */
  public CopycatConfig config() {
    return config;
  }

  /**
   * Returns the core Copycat cluster.
   *
   * @return The core Copycat cluster.
   */
  public Cluster cluster() {
    return context.cluster();
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
    return this.config.getClusterConfig().withProtocol(new CoordinatedProtocol(HashFunctions.CITYHASH.hash32(name.getBytes()), protocol, registry, executor));
  }

  /**
   * Creates a new resource.
   */
  @SuppressWarnings("unchecked")
  private <T extends Resource<T>, U extends ResourceConfig<U>> T createResource(U config, Executor executor, Function<ResourceContext, T> factory) {
    return (T) resources.computeIfAbsent(config.getName(), n -> {
      ScheduledExecutorService scheduler = createExecutor(n);
      return factory.apply(new ResourceContext(config, createClusterConfig(config.getName(), scheduler), scheduler, executor));
    });
  }

  /**
   * Creates a new event log.
   *
   * @param config The event log configuration.
   * @return The event log instance.
   */
  public <K, V> EventLog<K, V> createEventLog(EventLogConfig config) {
    return createResource(config, context.executor(), EventLog::new);
  }

  /**
   * Creates a new event log.
   *
   * @param config The event log configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The event log instance.
   */
  public <K, V> EventLog<K, V> createEventLog(EventLogConfig config, Executor executor) {
    return createResource(config, executor, EventLog::new);
  }

  /**
   * Creates a new state log.
   *
   * @param config The state log configuration.
   * @return The state log instance.
   */
  public <K, V> StateLog<K, V> createStateLog(StateLogConfig config) {
    return createResource(config, context.executor(), StateLog::new);
  }

  /**
   * Creates a new state log.
   *
   * @param config The state log configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The state log instance.
   */
  public <K, V> StateLog<K, V> createStateLog(StateLogConfig config, Executor executor) {
    return createResource(config, executor, StateLog::new);
  }

  /**
   * Creates a new replicated state machine.
   *
   * @param state The state machine state.
   * @param config The state machine configuration.
   * @param <T> The state machine state type.
   * @return The state machine instance.
   */
  public <T> StateMachine<T> createStateMachine(T state, StateMachineConfig config) {
    return createResource(config, context.executor(), context -> new StateMachine<>(state, context));
  }

  /**
   * Creates a new replicated state machine.
   *
   * @param state The state machine state.
   * @param config The state machine configuration.
   * @param executor An executor on which to execute callbacks.
   * @param <T> The state machine state type.
   * @return The state machine instance.
   */
  public <T> StateMachine<T> createStateMachine(T state, StateMachineConfig config, Executor executor) {
    return createResource(config, executor, context -> new StateMachine<>(state, context));
  }

  /**
   * Creates a new leader election.
   *
   * @param config The leader election configuration.
   * @return The leader election instance.
   */
  public LeaderElection createLeaderElection(LeaderElectionConfig config) {
    return createResource(config, context.executor(), LeaderElection::new);
  }

  /**
   * Creates a new leader election.
   *
   * @param config The leader election configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The leader election instance.
   */
  public LeaderElection createLeaderElection(LeaderElectionConfig config, Executor executor) {
    return createResource(config, executor, LeaderElection::new);
  }

  /**
   * Creates a named asynchronous map.
   *
   * @param config The map configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The asynchronous map instance.
   */
  public <K, V> AsyncMap<K, V> createMap(AsyncMapConfig config) {
    return createResource(config, context.executor(), AsyncMap::new);
  }

  /**
   * Creates a named asynchronous map.
   *
   * @param config The map configuration.
   * @param executor An executor on which to execute callbacks.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The asynchronous map instance.
   */
  public <K, V> AsyncMap<K, V> createMap(AsyncMapConfig config, Executor executor) {
    return createResource(config, executor, AsyncMap::new);
  }

  /**
   * Creates a named asynchronous multimap.
   *
   * @param config The multimap configuration.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return The asynchronous multimap instance.
   */
  public <K, V> AsyncMultiMap<K, V> createMultiMap(AsyncMultiMapConfig config) {
    return createResource(config, context.executor(), AsyncMultiMap::new);
  }

  /**
   * Creates a named asynchronous multimap.
   *
   * @param config The multimap configuration.
   * @param executor An executor on which to execute callbacks.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return The asynchronous multimap instance.
   */
  public <K, V> AsyncMultiMap<K, V> createMultiMap(AsyncMultiMapConfig config, Executor executor) {
    return createResource(config, executor, AsyncMultiMap::new);
  }

  /**
   * Creates a named asynchronous set.
   *
   * @param config The set configuration.
   * @param <T> The set entry type.
   * @return The asynchronous set instance.
   */
  public <T> AsyncSet<T> createSet(AsyncSetConfig config) {
    return createResource(config, context.executor(), AsyncSet::new);
  }

  /**
   * Creates a named asynchronous set.
   *
   * @param config The set configuration.
   * @param executor An executor on which to execute callbacks.
   * @param <T> The set entry type.
   * @return The asynchronous set instance.
   */
  public <T> AsyncSet<T> createSet(AsyncSetConfig config, Executor executor) {
    return createResource(config, executor, AsyncSet::new);
  }

  /**
   * Creates a named asynchronous atomic long value.
   *
   * @param config The long configuration.
   * @return The asynchronous atomic long instance.
   */
  public AsyncLong createLong(AsyncLongConfig config) {
    return createResource(config, context.executor(), AsyncLong::new);
  }

  /**
   * Creates a named asynchronous atomic long value.
   *
   * @param config The long configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The asynchronous atomic long instance.
   */
  public AsyncLong createLong(AsyncLongConfig config, Executor executor) {
    return createResource(config, executor, AsyncLong::new);
  }

  /**
   * Creates a named asynchronous atomic boolean value.
   *
   * @param config The boolean configuration.
   * @return The asynchronous atomic boolean instance.
   */
  public AsyncBoolean createBoolean(AsyncBooleanConfig config) {
    return createResource(config, context.executor(), AsyncBoolean::new);
  }

  /**
   * Creates a named asynchronous atomic boolean value.
   *
   * @param config The boolean configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The asynchronous atomic boolean instance.
   */
  public AsyncBoolean createBoolean(AsyncBooleanConfig config, Executor executor) {
    return createResource(config, executor, AsyncBoolean::new);
  }

  /**
   * Creates a named asynchronous atomic reference value.
   *
   * @param config The reference configuration.
   * @return The asynchronous atomic reference instance.
   */
  public <T> AsyncReference<T> createReference(AsyncReferenceConfig config) {
    return createResource(config, context.executor(), AsyncReference::new);
  }

  /**
   * Creates a named asynchronous atomic reference value.
   *
   * @param config The reference configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The asynchronous atomic reference instance.
   */
  public <T> AsyncReference<T> createReference(AsyncReferenceConfig config, Executor executor) {
    return createResource(config, executor, AsyncReference::new);
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

}
