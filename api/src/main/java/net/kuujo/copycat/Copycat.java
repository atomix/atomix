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
import net.kuujo.copycat.internal.DefaultCopycat;
import net.kuujo.copycat.state.StateLog;
import net.kuujo.copycat.state.StateLogConfig;
import net.kuujo.copycat.state.StateMachine;
import net.kuujo.copycat.state.StateMachineConfig;
import net.kuujo.copycat.util.Managed;

/**
 * Copycat.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Copycat extends Managed<Copycat> {

  /**
   * Creates a new Copycat instance with the default Copycat and cluster configuration.
   *
   * @return The Copycat instance.
   */
  static Copycat create() {
    return create(new CopycatConfig().withClusterConfig(new ClusterConfig()));
  }

  /**
   * Creates a new Copycat instance, overriding the default cluster configuration.
   *
   * @param cluster The global cluster configuration.
   * @return The Copycat instance.
   */
  static Copycat create(ClusterConfig cluster) {
    return create(new CopycatConfig().withClusterConfig(cluster));
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param config The global Copycat configuration.
   * @return The Copycat instance.
   */
  static Copycat create(CopycatConfig config) {
    return new DefaultCopycat(config);
  }

  /**
   * Returns the global Copycat configuration.
   *
   * @return The global Copycat configuration.
   */
  CopycatConfig config();

  /**
   * Returns the core Copycat cluster.
   *
   * @return The core Copycat cluster.
   */
  Cluster cluster();

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @param <T> the event log entry type.
   * @return The event log instance.
   */
  <T> EventLog<T> createEventLog(String name);

  /**
   * Creates a new event log.
   *
   * @param name The name of the event log to create.
   * @param config The event log configuration.
   * @param <T> The event log entry type.
   * @return The event log instance.
   */
  <T> EventLog<T> createEventLog(String name, EventLogConfig config);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param <T> The state log entry type.
   * @return The state log instance.
   */
  <T> StateLog<T> createStateLog(String name);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param config The state log configuration.
   * @param <T> The state log entry type.
   * @return The state log instance.
   */
  <T> StateLog<T> createStateLog(String name, StateLogConfig config);

  /**
   * Creates a new replicated state machine.
   *
   * @param name The name of the state machine to create.
   * @param stateType The state machine state type.
   * @param initialState The state machine's initial state.
   * @param <T> The state machine state type.
   * @return The state machine instance.
   */
  <T> StateMachine<T> createStateMachine(String name, Class<T> stateType, Class<? extends T> initialState);

  /**
   * Creates a new replicated state machine.
   *
   * @param name The name of the state machine to create.
   * @param config The state machine configuration.
   * @param <T> The state machine state type.
   * @return The state machine instance.
   */
  <T> StateMachine<T> createStateMachine(String name, StateMachineConfig config);

  /**
   * Creates a new leader election.
   *
   * @param name The leader election name.
   * @return The leader election instance.
   */
  LeaderElection createLeaderElection(String name);

  /**
   * Creates a new leader election.
   *
   * @param name The leader election name.
   * @param config The leader election configuration.
   * @return The leader election instance.
   */
  LeaderElection createLeaderElection(String name, LeaderElectionConfig config);

  /**
   * Creates a named asynchronous map.
   *
   * @param name The map name.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The asynchronous map instance.
   */
  <K, V> AsyncMap<K, V> createMap(String name);

  /**
   * Creates a named asynchronous map.
   *
   * @param name The map name.
   * @param config The map configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The asynchronous map instance.
   */
  <K, V> AsyncMap<K, V> createMap(String name, AsyncMapConfig config);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return The asynchronous multimap instance.
   */
  <K, V> AsyncMultiMap<K, V> createMultiMap(String name);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param config The multimap configuration.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return The asynchronous multimap instance.
   */
  <K, V> AsyncMultiMap<K, V> createMultiMap(String name, AsyncMultiMapConfig config);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param <T> The list entry type.
   * @return The asynchronous list instance.
   */
  <T> AsyncList<T> createList(String name);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param config The list configuration.
   * @param <T> The list entry type.
   * @return The asynchronous list instance.
   */
  <T> AsyncList<T> createList(String name, AsyncListConfig config);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param <T> The set entry type.
   * @return The asynchronous set instance.
   */
  <T> AsyncSet<T> createSet(String name);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param config The set configuration.
   * @param <T> The set entry type.
   * @return The asynchronous set instance.
   */
  <T> AsyncSet<T> createSet(String name, AsyncSetConfig config);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @return The asynchronous lock instance.
   */
  AsyncLock createLock(String name);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @param config The lock configuration.
   * @return The asynchronous lock instance.
   */
  AsyncLock createLock(String name, AsyncLockConfig config);

  /**
   * Creates a named asynchronous atomic long value.
   *
   * @param name The atomic long name.
   * @return The asynchronous atomic long instance.
   */
  AsyncAtomicLong createLong(String name);

  /**
   * Creates a named asynchronous atomic long value.
   *
   * @param name The atomic long name.
   * @param config The long configuration.
   * @return The asynchronous atomic long instance.
   */
  AsyncAtomicLong createLong(String name, AsyncAtomicLongConfig config);

  /**
   * Creates a named asynchronous atomic boolean value.
   *
   * @param name The atomic boolean name.
   * @return The asynchronous atomic boolean instance.
   */
  AsyncAtomicBoolean createBoolean(String name);

  /**
   * Creates a named asynchronous atomic boolean value.
   *
   * @param name The atomic boolean name.
   * @param config The boolean configuration.
   * @return The asynchronous atomic boolean instance.
   */
  AsyncAtomicBoolean createBoolean(String name, AsyncAtomicBooleanConfig config);

  /**
   * Creates a named asynchronous atomic reference value.
   *
   * @param name The atomic reference name.
   * @return The asynchronous atomic reference instance.
   */
  <T> AsyncAtomicReference<T> createReference(String name);

  /**
   * Creates a named asynchronous atomic reference value.
   *
   * @param name The atomic reference name.
   * @param config The reference configuration.
   * @return The asynchronous atomic reference instance.
   */
  <T> AsyncAtomicReference<T> createReference(String name, AsyncAtomicReferenceConfig config);

}
