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
import net.kuujo.copycat.resource.Resource;

/**
 * Asynchronous map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 */
public interface AsyncMap<K, V> extends AsyncMapProxy<K, V>, Resource<AsyncMap<K, V>> {

  /**
   * Creates a new asynchronous map with the default cluster configuration.<p>
   *
   * The map will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the map will be constructed with an map configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code map}, {@code map-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the map resource. If the resource is namespaced - e.g. `maps.my-map.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `maps.conf`.
   *
   * @param name The asynchronous map name.
   * @param uri The asynchronous map member URI.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The asynchronous map.
   */
  static <K, V> AsyncMap<K, V> create(String name, String uri) {
    return create(name, uri, new ClusterConfig(), new AsyncMapConfig());
  }

  /**
   * Creates a new asynchronous map.<p>
   *
   * The map will be constructed with an map configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code map}, {@code map-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the map resource. If the resource is namespaced - e.g. `maps.my-map.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `maps.conf`.
   *
   * @param name The asynchronous map name.
   * @param uri The asynchronous map member URI.
   * @param cluster The cluster configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The asynchronous map.
   */
  static <K, V> AsyncMap<K, V> create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new AsyncMapConfig());
  }

  /**
   * Creates a new asynchronous map.
   *
   * @param name The asynchronous map name.
   * @param uri The asynchronous map member URI.
   * @param cluster The cluster configuration.
   * @param config The map configuration.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return The asynchronous map.
   */
  static <K, V> AsyncMap<K, V> create(String name, String uri, ClusterConfig cluster, AsyncMapConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(uri, new CoordinatorConfig().withName(name).withClusterConfig(cluster));
    return coordinator.<AsyncMap<K, V>>getResource(name, config.resolve(cluster))
      .addStartupTask(() -> coordinator.open().thenApply(v -> null))
      .addShutdownTask(coordinator::close);
  }

}
