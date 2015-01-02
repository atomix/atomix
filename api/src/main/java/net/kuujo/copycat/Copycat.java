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

/**
 * Copycat.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Copycat extends Managed<Copycat> {

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param cluster The global cluster configuration.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, ClusterConfig cluster) {
    return create(uri, new CopycatConfig().withClusterConfig(cluster));
  }

  /**
   * Creates a new Copycat instance.
   *
   * @param uri The local member URI.
   * @param config The global Copycat configuration.
   * @return The Copycat instance.
   */
  static Copycat create(String uri, CopycatConfig config) {
    return new DefaultCopycat(uri, config);
  }

  /**
   * Returns the global Copycat configuration.
   *
   * @return The gloabl Copycat configuration.
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
   * @return A completable future to be completed once the event log has been created.
   */
  <T, U> EventLog<T, U> eventLog(String name);

  /**
   * Creates a new state log.
   *
   * @param name The name of the state log to create.
   * @param <T> The state log entry type.
   * @return A completable future to be completed once the state log has been created.
   */
  <T, U> StateLog<T, U> stateLog(String name);

  /**
   * Creates a new replicated state machine.
   *
   * @param name The name of the state machine to create.
   * @param stateType The state machine state type.
   * @param initialState The state machine's initial state.
   * @param <T> The state machine state type.
   * @return A completable future to be completed once the state machine has been created.
   */
  <T> StateMachine<T> stateMachine(String name, Class<T> stateType, T initialState);

  /**
   * Creates a new leader election.
   *
   * @param name The leader election name.
   * @return A completable future to be completed once the leader election has been created.
   */
  LeaderElection leaderElection(String name);

  /**
   * Creates a named asynchronous map.
   *
   * @param name The map name.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A completable future to be completed once the asynchronous map has been created.
   */
  <K, V> AsyncMap<K, V> map(String name);

  /**
   * Creates a named asynchronous multimap.
   *
   * @param name The multimap name.
   * @param <K> The map key type.
   * @param <V> The map entry type.
   * @return A completable future to be completed once the asynchronous multimap has been created.
   */
  <K, V> AsyncMultiMap<K, V> multiMap(String name);

  /**
   * Creates a named asynchronous list.
   *
   * @param name The list name.
   * @param <T> The list entry type.
   * @return A completable future to be completed once the asynchronous list has been created.
   */
  <T> AsyncList<T> list(String name);

  /**
   * Creates a named asynchronous set.
   *
   * @param name The set name.
   * @param <T> The set entry type.
   * @return A completable future to be completed once the asynchronous set has been created.
   */
  <T> AsyncSet<T> set(String name);

  /**
   * Creates a named asynchronous lock.
   *
   * @param name The lock name.
   * @return A completable future to be completed once the asynchronous lock has been created.
   */
  AsyncLock lock(String name);

}
