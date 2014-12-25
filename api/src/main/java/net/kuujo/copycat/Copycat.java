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
import net.kuujo.copycat.log.LogConfig;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

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
    return create(uri, new ClusterConfig(), new LogConfig(), null);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param executor The user execution context.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, Executor executor) {
    return create(uri, new ClusterConfig(), new LogConfig(), executor);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param cluster The global cluster configuration.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, ClusterConfig cluster) {
    return create(uri, cluster, new LogConfig(), null);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param cluster The global cluster configuration.
   * @param executor The user execution executor.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, ClusterConfig cluster, Executor executor) {
    return create(uri, cluster, new LogConfig(), executor);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param cluster The global cluster configuration.
   * @param config The log configuration.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, ClusterConfig cluster, LogConfig config) {
    return create(uri, cluster, config, null);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param cluster The global cluster configuration.
   * @param config The log configuration.
   * @param executor The user execution context.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, ClusterConfig cluster, LogConfig config, Executor executor) {
    return new DefaultCopycat(uri, cluster, config, executor != null ? executor : Executors.newSingleThreadExecutor());
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
   * @param executor The event log executor.
   * @param <T> the event log entry type.
   * @return A completable future to be completed once the event log has been created.
   */
  <T> CompletableFuture<EventLog<T>> eventLog(String name, Executor executor);

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
   * @param cluster The event log cluster configuration.
   * @param executor The event log executor.
   * @param <T> the event log entry type.
   * @return A completable future to be completed once the event log has been created.
   */
  <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster, Executor executor);

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @param config The event log configuration.
   * @param <T> the event log entry type.
   * @return A completable future to be completed once the event log has been created.
   */
  <T> CompletableFuture<EventLog<T>> eventLog(String name, LogConfig config);

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @param config The event log configuration.
   * @param executor The event log executor.
   * @param <T> the event log entry type.
   * @return A completable future to be completed once the event log has been created.
   */
  <T> CompletableFuture<EventLog<T>> eventLog(String name, LogConfig config, Executor executor);

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @param cluster The event log cluster configuration.
   * @param config The event log configuration.
   * @param <T> the event log entry type.
   * @return A completable future to be completed once the event log has been created.
   */
  <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster, LogConfig config);

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @param cluster The event log cluster configuration.
   * @param config The event log configuration.
   * @param executor The event log executor.
   * @param <T> the event log entry type.
   * @return A completable future to be completed once the event log has been created.
   */
  <T> CompletableFuture<EventLog<T>> eventLog(String name, ClusterConfig cluster, LogConfig config, Executor executor);

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
   * @param executor The state log executor.
   * @param <T> The state log entry type.
   * @return A completable future to be completed once the state log has been created.
   */
  <T> CompletableFuture<StateLog<T>> stateLog(String name, Executor executor);

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
   * @param cluster The state log cluster configuration.
   * @param executor The state log executor.
   * @param <T> The state log entry type.
   * @return A completable future to be completed once the state log has been created.
   */
  <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster, Executor executor);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param config The state log configuration.
   * @param <T> The state log entry type.
   * @return A completable future to be completed once the state log has been created.
   */
  <T> CompletableFuture<StateLog<T>> stateLog(String name, LogConfig config);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param config The state log configuration.
   * @param executor The state log executor.
   * @param <T> The state log entry type.
   * @return A completable future to be completed once the state log has been created.
   */
  <T> CompletableFuture<StateLog<T>> stateLog(String name, LogConfig config, Executor executor);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param cluster The state log cluster configuration.
   * @param config The state log configuration.
   * @param <T> The state log entry type.
   * @return A completable future to be completed once the state log has been created.
   */
  <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster, LogConfig config);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param cluster The state log cluster configuration.
   * @param config The state log configuration.
   * @param executor The state log executor.
   * @param <T> The state log entry type.
   * @return A completable future to be completed once the state log has been created.
   */
  <T> CompletableFuture<StateLog<T>> stateLog(String name, ClusterConfig cluster, LogConfig config, Executor executor);

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
   * @param executor The state machine's executor.
   * @param <T> The state machine state type.
   * @return A completable future to be completed once the state machine has been created.
   */
  <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, Executor executor);

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
   * @param cluster The state machine cluster configuration.
   * @param executor The state machine's executor.
   * @param <T> The state machine state type.
   * @return A completable future to be completed once the state machine has been created.
   */
  <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster, Executor executor);

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
  <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, LogConfig config);

  /**
   * Creates a new replicated state machine.
   *
   * @param name The name of the state machine to create.
   * @param stateType The state machine state type.
   * @param initialState The state machine's initial state.
   * @param config The state machine's log configuration.
   * @param executor The state machine's executor.
   * @param <T> The state machine state type.
   * @return A completable future to be completed once the state machine has been created.
   */
  <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, LogConfig config, Executor executor);

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
  <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster, LogConfig config);

  /**
   * Creates a new replicated state machine.
   *
   * @param name The name of the state machine to create.
   * @param stateType The state machine state type.
   * @param initialState The state machine's initial state.
   * @param cluster The state machine cluster configuration.
   * @param config The state machine's log configuration.
   * @param executor The state machine's executor.
   * @param <T> The state machine state type.
   * @return A completable future to be completed once the state machine has been created.
   */
  <T> CompletableFuture<StateMachine<T>> stateMachine(String name, Class<T> stateType, T initialState, ClusterConfig cluster, LogConfig config, Executor executor);

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
   * @param executor The leader election's executor.
   * @return A completable future to be completed once the leader election has been created.
   */
  CompletableFuture<LeaderElection> election(String name, Executor executor);

  /**
   * Creates a new leader election.
   *
   * @param name The leader election name.
   * @param cluster The leader election cluster configuration.
   * @return A completable future to be completed once the leader election has been created.
   */
  CompletableFuture<LeaderElection> election(String name, ClusterConfig cluster);

  /**
   * Creates a new leader election.
   *
   * @param name The leader election name.
   * @param cluster The leader election cluster configuration.
   * @param executor The leader election's executor.
   * @return A completable future to be completed once the leader election has been created.
   */
  CompletableFuture<LeaderElection> election(String name, ClusterConfig cluster, Executor executor);

  /**
   * Creates a named asynchronous map.
   *
   * @param name The map name.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> CompletableFuture<AsyncMap<K, V>> map(String name);

  /**
   * Creates a named asynchronous map.
   *
   * @param name The map name.
   * @param executor The map's executor.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, Executor executor);

  /**
   * Creates a named asynchronous map.
   *
   * @param name The map name.
   * @param cluster The map cluster configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster);

  /**
   * Creates a named asynchronous map.
   *
   * @param name The map name.
   * @param cluster The map cluster configuration.
   * @param executor The map's executor.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster, Executor executor);

  /**
   * Creates a name asynchronous map.
   *
   * @param name The map name.
   * @param config The map log configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, LogConfig config);

  /**
   * Creates a name asynchronous map.
   *
   * @param name The map name.
   * @param config The map log configuration.
   * @param executor The map's executor.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, LogConfig config, Executor executor);

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
  <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster, LogConfig config);

  /**
   * Creates a name asynchronous map.
   *
   * @param name The map name.
   * @param cluster The map cluster configuration.
   * @param config The map log configuration.
   * @param executor The map's executor.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> CompletableFuture<AsyncMap<K, V>> map(String name, ClusterConfig cluster, LogConfig config, Executor executor);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param executor The multimap's executor.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, Executor executor);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param cluster The multimap cluster configuration.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param cluster The multimap cluster configuration.
   * @param executor The multimap's executor.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster, Executor executor);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param config The log configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, LogConfig config);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param config The log configuration.
   * @param executor The multimap's executor.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, LogConfig config, Executor executor);

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
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster, LogConfig config);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param cluster The multimap cluster configuration.
   * @param config The log configuration.
   * @param executor The multimap's executor.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> CompletableFuture<AsyncMultiMap<K, V>> multiMap(String name, ClusterConfig cluster, LogConfig config, Executor executor);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> list(String name);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param executor The list's executor.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> list(String name, Executor executor);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param cluster The list cluster configuration.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param cluster The list cluster configuration.
   * @param executor The list's executor.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster, Executor executor);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param config The log configuration.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> list(String name, LogConfig config);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param config The log configuration.
   * @param executor The list's executor.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> list(String name, LogConfig config, Executor executor);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param cluster The list cluster configuration.
   * @param config The log configuration.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster, LogConfig config);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param cluster The list cluster configuration.
   * @param config The log configuration.
   * @param executor The list's executor.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> CompletableFuture<AsyncList<T>> list(String name, ClusterConfig cluster, LogConfig config, Executor executor);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> set(String name);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param executor The set's executor.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> set(String name, Executor executor);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param cluster The set cluster configuration.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param cluster The set cluster configuration.
   * @param executor The set's executor.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster, Executor executor);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param config The log configuration.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> set(String name, LogConfig config);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param config The log configuration.
   * @param executor The set's executor.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> set(String name, LogConfig config, Executor executor);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param cluster The set cluster configuration.
   * @param config The log configuration.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster, LogConfig config);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param cluster The set cluster configuration.
   * @param config The log configuration.
   * @param executor The set's executor.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> CompletableFuture<AsyncSet<T>> set(String name, ClusterConfig cluster, LogConfig config, Executor executor);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> lock(String name);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @param executor The lock's executor.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> lock(String name, Executor executor);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @param cluster The lock cluster configuration.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @param cluster The lock cluster configuration.
   * @param executor The lock's executor.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster, Executor executor);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @param config The log configuration.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> lock(String name, LogConfig config);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @param config The log configuration.
   * @param executor The lock's executor.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> lock(String name, LogConfig config, Executor executor);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @param cluster The lock cluster configuration.
   * @param config The log configuration.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster, LogConfig config);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @param cluster The lock cluster configuration.
   * @param config The log configuration.
   * @param executor The lock's executor.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  CompletableFuture<AsyncLock> lock(String name, ClusterConfig cluster, LogConfig config, Executor executor);

}
