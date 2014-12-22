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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.*;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.internal.DefaultCopycat;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;

/**
 * Copycat.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Copycat extends Managed {

  /**
   * Creates a new Copycat instance, loading the configuration from the classpath.
   *
   * @param uri The local member URI.
   * @return The Copycat instance.
   */
  static Copycat create(String uri) {
    return create(uri, new ClusterConfig(), new CopycatConfig(), null);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param context The user execution context.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, ExecutionContext context) {
    return create(uri, new ClusterConfig(), new CopycatConfig(), context);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param cluster The global cluster configuration.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, ClusterConfig cluster) {
    return create(uri, cluster, new CopycatConfig(), null);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param cluster The global cluster configuration.
   * @param context The user execution context.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, ClusterConfig cluster, ExecutionContext context) {
    return create(uri, cluster, new CopycatConfig(), context);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param cluster The global cluster configuration.
   * @param config The log configuration.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, ClusterConfig cluster, CopycatConfig config) {
    return create(uri, cluster, config, null);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param cluster The global cluster configuration.
   * @param config The log configuration.
   * @param context The user execution context.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, ClusterConfig cluster, CopycatConfig config, ExecutionContext context) {
    return new DefaultCopycat(uri, cluster, config, context != null ? context : ExecutionContext.create());
  }

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @param <T> the event log entry type.
   * @return A completable future to be completed once the event log has been created.
   */
  <T> CompletableFuture<EventLog<T>> eventLog(String name);

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @param cluster The event log cluster configuration.
   * @param <T> the event log entry type.
   * @return A completable future to be completed once the event log has been created.
   */
  <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster);

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @param config The event log configuration.
   * @param <T> the event log entry type.
   * @return A completable future to be completed once the event log has been created.
   */
  <T> CompletableFuture<EventLog<T>> eventLog(String name, EventLogConfig config);

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @param cluster The event log cluster configuration.
   * @param config The event log configuration.
   * @param <T> the event log entry type.
   * @return A completable future to be completed once the event log has been created.
   */
  <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster, EventLogConfig config);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param <T> The state log entry type.
   * @return A completable future to be completed once the state log has been created.
   */
  <T> CompletableFuture<StateLog<T>> stateLog(String name);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param cluster The state log cluster configuration.
   * @param <T> The state log entry type.
   * @return A completable future to be completed once the state log has been created.
   */
  <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param config The state log configuration.
   * @param <T> The state log entry type.
   * @return A completable future to be completed once the state log has been created.
   */
  <T> CompletableFuture<StateLog<T>> stateLog(String name, StateLogConfig config);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param cluster The state log cluster configuration.
   * @param config The state log configuration.
   * @param <T> The state log entry type.
   * @return A completable future to be completed once the state log has been created.
   */
  <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster, StateLogConfig config);

  /**
   * Creates a new replicated state machine.
   *
   * @param name The name of the state machine to create.
   * @param stateType The state machine state type.
   * @param initialState The state machine's initial state.
   * @param <T> The state machine state type.
   * @return A completable future to be completed once the state machine has been created.
   */
  <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState);

  /**
   * Creates a new replicated state machine.
   *
   * @param name The name of the state machine to create.
   * @param stateType The state machine state type.
   * @param initialState The state machine's initial state.
   * @param cluster The state machine cluster configuration.
   * @param <T> The state machine state type.
   * @return A completable future to be completed once the state machine has been created.
   */
  <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster);

  /**
   * Creates a new replicated state machine.
   *
   * @param name The name of the state machine to create.
   * @param stateType The state machine state type.
   * @param initialState The state machine's initial state.
   * @param config The state machine's log configuration.
   * @param <T> The state machine state type.
   * @return A completable future to be completed once the state machine has been created.
   */
  <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, StateMachineConfig config);

  /**
   * Creates a new replicated state machine.
   *
   * @param name The name of the state machine to create.
   * @param stateType The state machine state type.
   * @param initialState The state machine's initial state.
   * @param cluster The state machine cluster configuration.
   * @param config The state machine's log configuration.
   * @param <T> The state machine state type.
   * @return A completable future to be completed once the state machine has been created.
   */
  <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster, StateMachineConfig config);

  /**
   * Creates a new leader election.
   *
   * @param name The leader election name.
   * @return A completable future to be completed once the leader election has been created.
   */
  CompletableFuture<LeaderElection> election(String name);

  /**
   * Creates a new leader election.
   *
   * @param name The leader election name.
   * @param cluster The leader election cluster configuration.
   * @return A completable future to be completed once the leader election has been created.
   */
  CompletableFuture<LeaderElection> election(String name, ClusterConfig cluster);

  /**
   * Creates a named asynchronous map.
   *
   * @param name The map name.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> CompletableFuture<AsyncMap<K, V>> getMap(String name);

  /**
   * Creates a named asynchronous map.
   *
   * @param name The map name.
   * @param cluster The map cluster configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> CompletableFuture<AsyncMap<K, V>> getMap(String name, ClusterConfig cluster);

  /**
   * Creates a name asynchronous map.
   *
   * @param name The map name.
   * @param config The map log configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> CompletableFuture<AsyncMap<K, V>> getMap(String name, AsyncMapConfig config);

  /**
   * Creates a name asynchronous map.
   *
   * @param name The map name.
   * @param cluster The map cluster configuration.
   * @param config The map log configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> CompletableFuture<AsyncMap<K, V>> getMap(String name, ClusterConfig cluster, AsyncMapConfig config);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> getMultiMap(String name);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param cluster The multimap cluster configuration.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> getMultiMap(String name, ClusterConfig cluster);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param config The log configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> getMultiMap(String name, AsyncMultiMapConfig config);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param cluster The multimap cluster configuration.
   * @param config The log configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> getMultiMap(String name, ClusterConfig cluster, AsyncMultiMapConfig config);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> getList(String name);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param cluster The list cluster configuration.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> getList(String name, ClusterConfig cluster);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param config The log configuration.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> getList(String name, AsyncListConfig config);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param cluster The list cluster configuration.
   * @param config The log configuration.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> getList(String name, ClusterConfig cluster, AsyncListConfig config);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> getSet(String name);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param cluster The set cluster configuration.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> getSet(String name, ClusterConfig cluster);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param config The log configuration.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> getSet(String name, AsyncSetConfig config);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param cluster The set cluster configuration.
   * @param config The log configuration.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> getSet(String name, ClusterConfig cluster, AsyncSetConfig config);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> getLock(String name);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @param cluster The lock cluster configuration.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> getLock(String name, ClusterConfig cluster);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @param config The log configuration.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> getLock(String name, AsyncLockConfig config);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @param cluster The lock cluster configuration.
   * @param config The log configuration.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> getLock(String name, ClusterConfig cluster, AsyncLockConfig config);

}
