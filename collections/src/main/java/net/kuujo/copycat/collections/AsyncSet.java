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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatorConfig;
import net.kuujo.copycat.cluster.internal.coordinator.DefaultClusterCoordinator;

/**
 * Asynchronous set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The set data type.
 */
public interface AsyncSet<T> extends AsyncCollection<AsyncSet<T>, T>, AsyncSetProxy<T> {

  /**
   * Creates a new asynchronous set with the default cluster configuration.<p>
   *
   * The set will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the set will be constructed with an set configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code set}, {@code set-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the set resource. If the resource is namespaced - e.g. `sets.my-set.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `sets.conf`.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  static <T> AsyncSet<T> create(String name, String uri) {
    return create(name, uri, new ClusterConfig(), new AsyncSetConfig());
  }

  /**
   * Creates a new asynchronous set.<p>
   *
   * The set will be constructed with an set configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code set}, {@code set-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the set resource. If the resource is namespaced - e.g. `sets.my-set.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `sets.conf`.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param cluster The cluster configuration.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  static <T> AsyncSet<T> create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new AsyncSetConfig());
  }

  /**
   * Creates a new asynchronous set.
   *
   * @param name The asynchronous set name.
   * @param uri The asynchronous set member URI.
   * @param cluster The cluster configuration.
   * @param config The set configuration.
   * @param <T> The set data type.
   * @return The asynchronous set.
   */
  static <T> AsyncSet<T> create(String name, String uri, ClusterConfig cluster, AsyncSetConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(uri, new CoordinatorConfig().withName(name).withClusterConfig(cluster));
    return coordinator.<AsyncSet<T>>getResource(name, config.resolve(cluster))
      .addStartupTask(() -> coordinator.open().thenApply(v -> null))
      .addShutdownTask(coordinator::close);
  }

}
