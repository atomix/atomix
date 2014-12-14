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

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.*;
import net.kuujo.copycat.election.LeaderElection;
import net.kuujo.copycat.internal.DefaultCopycat;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.spi.ExecutionContext;

/**
 * Copycat.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Copycat extends Managed {

  /**
   * Creates a new Copycat instance, loading the configuration from the classpath.
   *
   * @return The Copycat instance.
   */
  static Copycat create() {
    return create(Services.load("copycat.cluster", ClusterConfig.class), Services.load("copycat.log", CopycatConfig.class), null);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param context The user execution context.
   * @return The Copycat instance.
   */
  static Copycat create(ExecutionContext context) {
    return create(Services.load("copycat.cluster", ClusterConfig.class), Services.load("copycat.log", CopycatConfig.class), context);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param cluster The global cluster configuration.
   * @return The Copycat instance.
   */
  static Copycat create(ClusterConfig cluster) {
    return create(cluster, Services.load("copycat.log", CopycatConfig.class), null);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param cluster The global cluster configuration.
   * @param context The user execution context.
   * @return The Copycat instance.
   */
  static Copycat create(ClusterConfig cluster, ExecutionContext context) {
    return create(cluster, Services.load("copycat.log", CopycatConfig.class), context);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param cluster The global cluster configuration.
   * @param config The log configuration.
   * @return The Copycat instance.
   */
  static Copycat create(ClusterConfig cluster, CopycatConfig config) {
    return create(cluster, config, null);
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param cluster The global cluster configuration.
   * @param config The log configuration.
   * @param context The user execution context.
   * @return The Copycat instance.
   */
  static Copycat create(ClusterConfig cluster, CopycatConfig config, ExecutionContext context) {
    return new DefaultCopycat(cluster, config, context != null ? context : ExecutionContext.create());
  }

  /**
   * Returns the Copycat cluster.
   *
   * @return The Copycat cluster.
   */
  Cluster cluster();

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @return The event log.
   */
  <T> EventLog<T> eventLog(String name);

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @param config The event log configuration.
   * @return The event log.
   */
  <T> EventLog<T> eventLog(String name, EventLogConfig config);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @return The state log.
   */
  <T> StateLog<T> stateLog(String name);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param config The state log configuration.
   * @return The state log.
   */
  <T> StateLog<T> stateLog(String name, StateLogConfig config);

  /**
   * Creates a new replicated state machine.
   *
   * @param name The name of the state machine to create.
   * @param initialState The state machine's initial state.
   * @return A completable future to be completed once the state machine has been created.
   */
  <T> StateMachine<T> stateMachine(String name, Class<T> stateType, T initialState);

  /**
   * Creates a new replicated state machine.
   *
   * @param name The name of the state machine to create.
   * @param initialState The state machine's initial state.
   * @param config The state machine's log configuration.
   * @return A completable future to be completed once the state machine has been created.
   */
  <T> StateMachine<T> stateMachine(String name, Class<T> stateType, T initialState, StateMachineConfig config);

  /**
   * Creates a new leader election.
   *
   * @param name The leader election name.
   * @return A completable future to be completed once the leader election has been created.
   */
  LeaderElection election(String name);

  /**
   * Returns a named asynchronous map.
   *
   * @param name The map name.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return An asynchronous map.
   */
  <K, V> AsyncMap<K, V> getMap(String name);

  /**
   * Returns a name asynchronous map.
   *
   * @param name The map name.
   * @param config The map log configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return An asynchronous map.
   */
  <K, V> AsyncMap<K, V> getMap(String name, AsyncMapConfig config);

  /**
   * Returns a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return An asynchronous multimap.
   */
  <K, V> AsyncMultiMap<K, V> getMultiMap(String name);

  /**
   * Returns a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param config The log configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return An asynchronous multimap.
   */
  <K, V> AsyncMultiMap<K, V> getMultiMap(String name, AsyncMultiMapConfig config);

  /**
   * Returns a named asynchronous list.
   *
   * @param name The list name.
   * @param <T> The list entry type.
   * @return An asynchronous list.
   */
  <T> AsyncList<T> getList(String name);

  /**
   * Returns a named asynchronous list.
   *
   * @param name The list name.
   * @param config The log configuration.
   * @param <T> The list entry type.
   * @return An asynchronous list.
   */
  <T> AsyncList<T> getList(String name, AsyncListConfig config);

  /**
   * Returns a named asynchronous set.
   *
   * @param name The set name.
   * @param <T> The set entry type.
   * @return An asynchronous set.
   */
  <T> AsyncSet<T> getSet(String name);

  /**
   * Returns a named asynchronous set.
   *
   * @param name The set name.
   * @param config The log configuration.
   * @param <T> The set entry type.
   * @return An asynchronous set.
   */
  <T> AsyncSet<T> getSet(String name, AsyncSetConfig config);

  /**
   * Returns a named asynchronous lock.
   *
   * @param name The lock name.
   * @return An asynchronous lock.
   */
  AsyncLock getLock(String name);

  /**
   * Returns a named asynchronous lock.
   *
   * @param name The lock name.
   * @param config The log configuration.
   * @return An asynchronous lock.
   */
  AsyncLock getLock(String name, AsyncLockConfig config);

}
