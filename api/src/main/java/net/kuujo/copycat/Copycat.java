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

import java.util.concurrent.Executor;

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
   * Creates a new Copycat instance with the default Copycat and cluster configuration.
   *
   * @param executor An executor on which to execute callbacks.
   * @return The Copycat instance.
   */
  static Copycat create(Executor executor) {
    return create(new CopycatConfig().withClusterConfig(new ClusterConfig()), executor);
  }

  /**
   * Creates a new Copycat instance with a named configuration.
   *
   * @param name The Copycat configuration resource name.
   * @return The Copycat instance.
   */
  static Copycat create(String name) {
    return create(new CopycatConfig(name).withClusterConfig(new ClusterConfig()));
  }

  /**
   * Creates a new Copycat instance with the default Copycat and cluster configuration.
   *
   * @param name The Copycat configuration resource name.
   * @param executor An executor on which to execute callbacks.
   * @return The Copycat instance.
   */
  static Copycat create(String name, Executor executor) {
    return create(new CopycatConfig(name).withClusterConfig(new ClusterConfig()), executor);
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
   * Creates a new Copycat instance, overriding the default cluster configuration.
   *
   * @param cluster The global cluster configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The Copycat instance.
   */
  static Copycat create(ClusterConfig cluster, Executor executor) {
    return create(new CopycatConfig().withClusterConfig(cluster), executor);
  }

  /**
   * Creates a new Copycat instance, overriding the default cluster configuration.
   *
   * @param name The Copycat configuration resource name.
   * @param cluster The global cluster configuration.
   * @return The Copycat instance.
   */
  static Copycat create(String name, ClusterConfig cluster) {
    return create(new CopycatConfig(name).withClusterConfig(cluster));
  }

  /**
   * Creates a new Copycat instance, overriding the default cluster configuration.
   *
   * @param name The Copycat configuration resource name.
   * @param cluster The global cluster configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The Copycat instance.
   */
  static Copycat create(String name, ClusterConfig cluster, Executor executor) {
    return create(new CopycatConfig(name).withClusterConfig(cluster), executor);
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
   * Creates a new Copycat instance.
   *
   * @param config The global Copycat configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The Copycat instance.
   */
  static Copycat create(CopycatConfig config, Executor executor) {
    return new DefaultCopycat(config, executor);
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
   * Creates a new event log from a named resource configuration.
   *
   * @param name The event log resource name.
   * @param <T> the event log entry type.
   * @return The event log instance.
   */
  <T> EventLog<T> createEventLog(String name);

  /**
   * Creates a new event log from a named resource configuration.
   *
   * @param name The event log resource name.
   * @param executor An executor on which to execute callbacks.
   * @param <T> the event log entry type.
   * @return The event log instance.
   */
  <T> EventLog<T> createEventLog(String name, Executor executor);

  /**
   * Creates a new event log.
   *
   * @param config The event log configuration.
   * @param <T> The event log entry type.
   * @return The event log instance.
   */
  <T> EventLog<T> createEventLog(EventLogConfig config);

  /**
   * Creates a new event log.
   *
   * @param config The event log configuration.
   * @param executor An executor on which to execute callbacks.
   * @param <T> The event log entry type.
   * @return The event log instance.
   */
  <T> EventLog<T> createEventLog(EventLogConfig config, Executor executor);

  /**
   * Creates a new state log from a named resource configuration.
   *
   * @param name The state log resource name.
   * @param <T> The state log entry type.
   * @return The state log instance.
   */
  <T> StateLog<T> createStateLog(String name);

  /**
   * Creates a new state log from a named resource configuration.
   *
   * @param name The state log resource name.
   * @param executor An executor on which to execute callbacks.
   * @param <T> The state log entry type.
   * @return The state log instance.
   */
  <T> StateLog<T> createStateLog(String name, Executor executor);

  /**
   * Creates a new state log.
   *
   * @param config The state log configuration.
   * @param <T> The state log entry type.
   * @return The state log instance.
   */
  <T> StateLog<T> createStateLog(StateLogConfig config);

  /**
   * Creates a new state log.
   *
   * @param config The state log configuration.
   * @param executor An executor on which to execute callbacks.
   * @param <T> The state log entry type.
   * @return The state log instance.
   */
  <T> StateLog<T> createStateLog(StateLogConfig config, Executor executor);

  /**
   * Creates a new replicated state machine from a named resource configuration.
   *
   * @param name The state machine resource name.
   * @param stateType The state machine state type.
   * @param initialState The state machine's initial state.
   * @param <T> The state machine state type.
   * @return The state machine instance.
   */
  <T> StateMachine<T> createStateMachine(String name, Class<T> stateType, Class<? extends T> initialState);

  /**
   * Creates a new replicated state machine from a named resource configuration.
   *
   * @param name The state machine resource name.
   * @param stateType The state machine state type.
   * @param initialState The state machine's initial state.
   * @param executor An executor on which to execute callbacks.
   * @param <T> The state machine state type.
   * @return The state machine instance.
   */
  <T> StateMachine<T> createStateMachine(String name, Class<T> stateType, Class<? extends T> initialState, Executor executor);

  /**
   * Creates a new replicated state machine.
   *
   * @param config The state machine configuration.
   * @param <T> The state machine state type.
   * @return The state machine instance.
   */
  <T> StateMachine<T> createStateMachine(StateMachineConfig config);

  /**
   * Creates a new replicated state machine.
   *
   * @param config The state machine configuration.
   * @param executor An executor on which to execute callbacks.
   * @param <T> The state machine state type.
   * @return The state machine instance.
   */
  <T> StateMachine<T> createStateMachine(StateMachineConfig config, Executor executor);

  /**
   * Creates a new leader election from a named resource configuration.
   *
   * @param name The leader election resource name.
   * @return The leader election instance.
   */
  LeaderElection createLeaderElection(String name);

  /**
   * Creates a new leader election from a named resource configuration.
   *
   * @param name The leader election resource name.
   * @param executor An executor on which to execute callbacks.
   * @return The leader election instance.
   */
  LeaderElection createLeaderElection(String name, Executor executor);

  /**
   * Creates a new leader election.
   *
   * @param config The leader election configuration.
   * @return The leader election instance.
   */
  LeaderElection createLeaderElection(LeaderElectionConfig config);

  /**
   * Creates a new leader election.
   *
   * @param config The leader election configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The leader election instance.
   */
  LeaderElection createLeaderElection(LeaderElectionConfig config, Executor executor);

  /**
   * Creates a named asynchronous map from a named resource configuration.
   *
   * @param name The map resource name.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The asynchronous map instance.
   */
  <K, V> AsyncMap<K, V> createMap(String name);

  /**
   * Creates a named asynchronous map from a named resource configuration.
   *
   * @param name The map resource name.
   * @param executor An executor on which to execute callbacks.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The asynchronous map instance.
   */
  <K, V> AsyncMap<K, V> createMap(String name, Executor executor);

  /**
   * Creates a named asynchronous map.
   *
   * @param config The map configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The asynchronous map instance.
   */
  <K, V> AsyncMap<K, V> createMap(AsyncMapConfig config);

  /**
   * Creates a named asynchronous map.
   *
   * @param config The map configuration.
   * @param executor An executor on which to execute callbacks.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The asynchronous map instance.
   */
  <K, V> AsyncMap<K, V> createMap(AsyncMapConfig config, Executor executor);

  /**
   * Creates a named asynchronous multimap from a named resource configuration.
   *
   * @param name The multimap resource name.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return The asynchronous multimap instance.
   */
  <K, V> AsyncMultiMap<K, V> createMultiMap(String name);

  /**
   * Creates a named asynchronous multimap from a named resource configuration.
   *
   * @param name The multimap resource name.
   * @param executor An executor on which to execute callbacks.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return The asynchronous multimap instance.
   */
  <K, V> AsyncMultiMap<K, V> createMultiMap(String name, Executor executor);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param config The multimap configuration.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return The asynchronous multimap instance.
   */
  <K, V> AsyncMultiMap<K, V> createMultiMap(AsyncMultiMapConfig config);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param config The multimap configuration.
   * @param executor An executor on which to execute callbacks.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return The asynchronous multimap instance.
   */
  <K, V> AsyncMultiMap<K, V> createMultiMap(AsyncMultiMapConfig config, Executor executor);

  /**
   * Creates a named asynchronous list from a named resource configuration.
   *
   * @param name The list resource name.
   * @param <T> The list entry type.
   * @return The asynchronous list instance.
   */
  <T> AsyncList<T> createList(String name);

  /**
   * Creates a named asynchronous list from a named resource configuration.
   *
   * @param name The list resource name.
   * @param executor An executor on which to execute callbacks.
   * @param <T> The list entry type.
   * @return The asynchronous list instance.
   */
  <T> AsyncList<T> createList(String name, Executor executor);

  /**
   * Creates a named asynchronous list.
   *
   * @param config The list configuration.
   * @param <T> The list entry type.
   * @return The asynchronous list instance.
   */
  <T> AsyncList<T> createList(AsyncListConfig config);

  /**
   * Creates a named asynchronous list.
   *
   * @param config The list configuration.
   * @param executor An executor on which to execute callbacks.
   * @param <T> The list entry type.
   * @return The asynchronous list instance.
   */
  <T> AsyncList<T> createList(AsyncListConfig config, Executor executor);

  /**
   * Creates a named asynchronous set from a named resource configuration.
   *
   * @param name The set resource name.
   * @param <T> The set entry type.
   * @return The asynchronous set instance.
   */
  <T> AsyncSet<T> createSet(String name);

  /**
   * Creates a named asynchronous set from a named resource configuration.
   *
   * @param name The set resource name.
   * @param executor An executor on which to execute callbacks.
   * @param <T> The set entry type.
   * @return The asynchronous set instance.
   */
  <T> AsyncSet<T> createSet(String name, Executor executor);

  /**
   * Creates a named asynchronous set.
   *
   * @param config The set configuration.
   * @param <T> The set entry type.
   * @return The asynchronous set instance.
   */
  <T> AsyncSet<T> createSet(AsyncSetConfig config);

  /**
   * Creates a named asynchronous set.
   *
   * @param config The set configuration.
   * @param executor An executor on which to execute callbacks.
   * @param <T> The set entry type.
   * @return The asynchronous set instance.
   */
  <T> AsyncSet<T> createSet(AsyncSetConfig config, Executor executor);

  /**
   * Creates a named asynchronous atomic long value from a named resource configuration.
   *
   * @param name The atomic long resource name.
   * @return The asynchronous atomic long instance.
   */
  AsyncLong createLong(String name);

  /**
   * Creates a named asynchronous atomic long value from a named resource configuration.
   *
   * @param name The atomic long resource name.
   * @param executor An executor on which to execute callbacks.
   * @return The asynchronous atomic long instance.
   */
  AsyncLong createLong(String name, Executor executor);

  /**
   * Creates a named asynchronous atomic long value.
   *
   * @param config The long configuration.
   * @return The asynchronous atomic long instance.
   */
  AsyncLong createLong(AsyncLongConfig config);

  /**
   * Creates a named asynchronous atomic long value.
   *
   * @param config The long configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The asynchronous atomic long instance.
   */
  AsyncLong createLong(AsyncLongConfig config, Executor executor);

  /**
   * Creates a named asynchronous atomic boolean value from a named resource configuration.
   *
   * @param name The atomic boolean resource name.
   * @return The asynchronous atomic boolean instance.
   */
  AsyncBoolean createBoolean(String name);

  /**
   * Creates a named asynchronous atomic boolean value from a named resource configuration.
   *
   * @param name The atomic boolean resource name.
   * @param executor An executor on which to execute callbacks.
   * @return The asynchronous atomic boolean instance.
   */
  AsyncBoolean createBoolean(String name, Executor executor);

  /**
   * Creates a named asynchronous atomic boolean value.
   *
   * @param config The boolean configuration.
   * @return The asynchronous atomic boolean instance.
   */
  AsyncBoolean createBoolean(AsyncBooleanConfig config);

  /**
   * Creates a named asynchronous atomic boolean value.
   *
   * @param config The boolean configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The asynchronous atomic boolean instance.
   */
  AsyncBoolean createBoolean(AsyncBooleanConfig config, Executor executor);

  /**
   * Creates a named asynchronous atomic reference value from a named resource configuration.
   *
   * @param name The atomic reference resource name.
   * @return The asynchronous atomic reference instance.
   */
  <T> AsyncReference<T> createReference(String name);

  /**
   * Creates a named asynchronous atomic reference value from a named resource configuration.
   *
   * @param name The atomic reference resource name.
   * @param executor An executor on which to execute callbacks.
   * @return The asynchronous atomic reference instance.
   */
  <T> AsyncReference<T> createReference(String name, Executor executor);

  /**
   * Creates a named asynchronous atomic reference value.
   *
   * @param config The reference configuration.
   * @return The asynchronous atomic reference instance.
   */
  <T> AsyncReference<T> createReference(AsyncReferenceConfig config);

  /**
   * Creates a named asynchronous atomic reference value.
   *
   * @param config The reference configuration.
   * @param executor An executor on which to execute callbacks.
   * @return The asynchronous atomic reference instance.
   */
  <T> AsyncReference<T> createReference(AsyncReferenceConfig config, Executor executor);

}
