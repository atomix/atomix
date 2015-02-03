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
 * Asynchronous atomic long.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncAtomicLong extends AsyncAtomicLongProxy, Resource<AsyncAtomicLong> {

  /**
   * Creates a new asynchronous atomic long with the default cluster configuration.<p>
   *
   * The atomic long will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the atomic long will be constructed with an atomic long configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code atomic}, {@code atomic-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the atomic long resource. If the resource is namespaced - e.g. `longs.my-long.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `longs.conf`.
   *
   * @param name The asynchronous atomic long name.
   * @param uri The asynchronous atomic long member URI.
   * @return The asynchronous atomic long.
   */
  static AsyncAtomicLong create(String name, String uri) {
    return create(name, uri, new ClusterConfig(), new AsyncAtomicLongConfig());
  }

  /**
   * Creates a new asynchronous atomic long with the default atomic long configuration.<p>
   *
   * The atomic long will be constructed with an atomic long configuration that searches the classpath for three
   * configuration files - {@code {name}}, {@code atomic}, {@code atomic-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the atomic long resource. If the resource is namespaced - e.g. `longs.my-long.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `longs.conf`.
   *
   * @param name The asynchronous atomic long name.
   * @param uri The asynchronous atomic long member URI.
   * @param cluster The cluster configuration.
   * @return The asynchronous atomic long.
   */
  static AsyncAtomicLong create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new AsyncAtomicLongConfig());
  }

  /**
   * Creates a new asynchronous atomic long.
   *
   * @param name The asynchronous atomic long name.
   * @param uri The asynchronous atomic long member URI.
   * @param cluster The cluster configuration.
   * @param config The atomic long configuration.
   * @return The asynchronous atomic long.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static AsyncAtomicLong create(String name, String uri, ClusterConfig cluster, AsyncAtomicLongConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(uri, new CoordinatorConfig().withName(name).withClusterConfig(cluster));
    AsyncAtomicLong reference = coordinator.getResource(name, config.resolve(cluster));
    ((Resource) reference).addStartupTask(() -> coordinator.open().thenApply(v -> null));
    ((Resource) reference).addShutdownTask(coordinator::close);
    return reference;
  }

}
