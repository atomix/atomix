/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.state;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.state.internal.DefaultStateMachine;

import java.util.concurrent.Executor;

/**
 * State machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateMachine<T> extends Resource<StateMachine<T>> {

  /**
   * Creates a new state machine, loading the log configuration from the classpath.
   *
   * @param <T> The state machine entry type.
   * @return A new state machine instance.
   */
  static <T> StateMachine<T> create() {
    return create(new StateMachineConfig(), new ClusterConfig());
  }

  /**
   * Creates a new state machine, loading the log configuration from the classpath.
   *
   * @param <T> The state machine entry type.
   * @return A new state machine instance.
   */
  static <T> StateMachine<T> create(Executor executor) {
    return create(new StateMachineConfig(), new ClusterConfig(), executor);
  }

  /**
   * Creates a new state machine, loading the log configuration from the classpath.
   *
   * @param name The state machine resource name to be used to load the state machine configuration from the classpath.
   * @param <T> The state machine entry type.
   * @return A new state machine instance.
   */
  static <T> StateMachine<T> create(String name) {
    return create(new StateMachineConfig(name), new ClusterConfig(String.format("cluster.%s", name)));
  }

  /**
   * Creates a new state machine, loading the log configuration from the classpath.
   *
   * @param name The state machine resource name to be used to load the state machine configuration from the classpath.
   * @param executor An executor on which to execute state machine callbacks.
   * @param <T> The state machine entry type.
   * @return A new state machine instance.
   */
  static <T> StateMachine<T> create(String name, Executor executor) {
    return create(new StateMachineConfig(name), new ClusterConfig(String.format("cluster.%s", name)), executor);
  }

  /**
   * Creates a new state machine with the given cluster and state machine configurations.
   *
   * @param name The state machine resource name to be used to load the state machine configuration from the classpath.
   * @param cluster The cluster configuration.
   * @return A new state machine instance.
   */
  static <T> StateMachine<T> create(String name, ClusterConfig cluster) {
    return create(new StateMachineConfig(name), cluster);
  }

  /**
   * Creates a new state machine with the given cluster and state machine configurations.
   *
   * @param name The state machine resource name to be used to load the state machine configuration from the classpath.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute state machine callbacks.
   * @return A new state machine instance.
   */
  static <T> StateMachine<T> create(String name, ClusterConfig cluster, Executor executor) {
    return create(new StateMachineConfig(name), cluster, executor);
  }

  /**
   * Creates a new state machine with the given cluster and state machine configurations.
   *
   * @param config The state machine configuration.
   * @param cluster The cluster configuration.
   * @return A new state machine instance.
   */
  static <T> StateMachine<T> create(StateMachineConfig config, ClusterConfig cluster) {
    return new DefaultStateMachine<>(config, cluster);
  }

  /**
   * Creates a new state machine with the given cluster and state machine configurations.
   *
   * @param config The state machine configuration.
   * @param cluster The cluster configuration.
   * @param executor An executor on which to execute state machine callbacks.
   * @return A new state machine instance.
   */
  static <T> StateMachine<T> create(StateMachineConfig config, ClusterConfig cluster, Executor executor) {
    return new DefaultStateMachine<>(config, cluster, executor);
  }

  /**
   * Creates a status machine proxy.
   *
   * @param type The proxy interface.
   * @param <U> The proxy type.
   * @return The proxy object.
   */
  <U> U createProxy(Class<U> type);

}
