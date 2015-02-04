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
import net.kuujo.copycat.cluster.internal.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatorConfig;
import net.kuujo.copycat.cluster.internal.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.resource.Resource;

/**
 * State machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateMachine<T> extends Resource<StateMachine<T>> {

  /**
   * Creates a new state machine with the default cluster and state machine configurations.<p>
   *
   * The state machine will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the state machine will be constructed with an state machine configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code state-machine}, {@code state-machine-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the state machine resource. If the resource is namespaced - e.g. `state-machines.my-machine.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `state-machines.conf`.
   *
   * @param name The state machine resource name.
   * @param stateType The state machine state type.
   * @param initialState The state machine state.
   * @return The state machine.
   */
  static <T> StateMachine<T> create(String name, Class<T> stateType, Class<? extends T> initialState) {
    return create(name, new ClusterConfig(), new StateMachineConfig().withStateType(stateType).withInitialState(initialState));
  }

  /**
   * Creates a new state machine with the default state machine configuration.<p>
   *
   * The state machine will be constructed with an state machine configuration that searches the classpath for three
   * configuration files - {@code {name}}, {@code state-machine}, {@code state-machine-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the state machine resource. If the resource is namespaced - e.g. `state-machines.my-machine.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `state-machines.conf`.
   *
   * @param name The state machine resource name.
   * @param stateType The state machine state type.
   * @param initialState The state machine state.
   * @param cluster The state machine cluster configuration.
   * @return The state machine.
   */
  static <T> StateMachine<T> create(String name, Class<T> stateType, Class<? extends T> initialState, ClusterConfig cluster) {
    return create(name, cluster, new StateMachineConfig().withStateType(stateType).withInitialState(initialState));
  }

  /**
   * Creates a new state machine.
   *
   * @param name The state machine resource name.
   * @param cluster The state machine cluster configuration.
   * @param config The state machine configuration.
   * @return The state machine.
   */
  static <T> StateMachine<T> create(String name, ClusterConfig cluster, StateMachineConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(new CoordinatorConfig().withName(name).withClusterConfig(cluster));
    return coordinator.<StateMachine<T>>getResource(name, config.resolve(cluster))
      .addStartupTask(() -> coordinator.open().thenApply(v -> null))
      .addShutdownTask(coordinator::close);
  }

  /**
   * Creates a state machine proxy.
   *
   * @param type The proxy interface.
   * @param <U> The proxy type.
   * @return The proxy object.
   */
  <U> U createProxy(Class<U> type);

}
