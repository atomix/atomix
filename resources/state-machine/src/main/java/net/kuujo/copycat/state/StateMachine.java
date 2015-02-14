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
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.state.internal.DefaultStateMachine;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;

import java.util.concurrent.Executors;

/**
 * State machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateMachine<T> extends Resource<StateMachine<T>> {

  /**
   * Creates a new status machine with the default cluster and status machine configurations.<p>
   *
   * The status machine will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the status machine will be constructed with an status machine configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code status-machine}, {@code status-machine-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the status machine resource. If the resource is namespaced - e.g. `status-machines.my-machine.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `status-machines.conf`.
   *
   * @param name The status machine resource name.
   * @param stateType The status machine status type.
   * @param initialState The status machine status.
   * @return The status machine.
   */
  static <T> StateMachine<T> create(String name, Class<T> stateType, Class<? extends T> initialState) {
    return create(name, new ClusterConfig(), new StateMachineConfig().withStateType(stateType).withInitialState(initialState));
  }

  /**
   * Creates a new status machine with the default status machine configuration.<p>
   *
   * The status machine will be constructed with an status machine configuration that searches the classpath for three
   * configuration files - {@code {name}}, {@code status-machine}, {@code status-machine-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the status machine resource. If the resource is namespaced - e.g. `status-machines.my-machine.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `status-machines.conf`.
   *
   * @param name The status machine resource name.
   * @param stateType The status machine status type.
   * @param initialState The status machine status.
   * @param cluster The status machine cluster configuration.
   * @return The status machine.
   */
  static <T> StateMachine<T> create(String name, Class<T> stateType, Class<? extends T> initialState, ClusterConfig cluster) {
    return create(name, cluster, new StateMachineConfig().withStateType(stateType).withInitialState(initialState));
  }

  /**
   * Creates a new status machine.
   *
   * @param name The status machine resource name.
   * @param cluster The status machine cluster configuration.
   * @param config The status machine configuration.
   * @return The status machine.
   */
  static <T> StateMachine<T> create(String name, ClusterConfig cluster, StateMachineConfig config) {
    return new DefaultStateMachine<>(new ResourceContext(name, config, cluster, Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-" + name + "-%d"))));
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
