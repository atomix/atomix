/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.atomic;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatorConfig;
import net.kuujo.copycat.cluster.internal.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.resource.Resource;

/**
 * Asynchronous atomic boolean.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncAtomicBoolean extends AsyncAtomicBooleanProxy, Resource<AsyncAtomicBoolean> {

  /**
   * Creates a new asynchronous atomic boolean with the default cluster configuration.<p>
   *
   * The atomic boolean will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the atomic boolean will be constructed with an atomic boolean configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code atomic}, {@code atomic-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the atomic boolean resource. If the resource is namespaced - e.g. `booleans.my-boolean.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `booleans.conf`.
   *
   * @param name The asynchronous atomic boolean name.
   * @return The asynchronous atomic boolean.
   */
  static AsyncAtomicBoolean create(String name) {
    return create(name, new ClusterConfig(String.format("%s-cluster", name)), new AsyncAtomicBooleanConfig(name));
  }

  /**
   * Creates a new asynchronous atomic boolean with the default atomic boolean configuration.<p>
   *
   * The atomic boolean will be constructed with an atomic boolean configuration that searches the classpath for three
   * configuration files - {@code {name}}, {@code atomic}, {@code atomic-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the atomic boolean resource. If the resource is namespaced - e.g. `booleans.my-boolean.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `booleans.conf`.
   *
   * @param name The asynchronous atomic boolean name.
   * @param cluster The cluster configuration.
   * @return The asynchronous atomic boolean.
   */
  static AsyncAtomicBoolean create(String name, ClusterConfig cluster) {
    return create(name, cluster, new AsyncAtomicBooleanConfig(name));
  }

  /**
   * Creates a new asynchronous atomic boolean.
   *
   * @param name The asynchronous atomic boolean name.
   * @param cluster The cluster configuration.
   * @param config The atomic boolean configuration.
   * @return The asynchronous atomic boolean.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static AsyncAtomicBoolean create(String name, ClusterConfig cluster, AsyncAtomicBooleanConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(new CoordinatorConfig().withName(name).withClusterConfig(cluster));
    return coordinator.<AsyncAtomicBoolean>getResource(name, config.resolve(cluster))
      .addStartupTask(() -> coordinator.open().thenApply(v -> null))
      .addShutdownTask(coordinator::close);
  }

}
