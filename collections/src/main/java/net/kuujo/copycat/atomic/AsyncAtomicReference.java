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
 * Asynchronous atomic value.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncAtomicReference<T> extends AsyncAtomicReferenceProxy<T>, Resource<AsyncAtomicReference<T>> {

  /**
   * Creates a new asynchronous atomic reference with the default cluster configuration.<p>
   *
   * The atomic reference will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the atomic reference will be constructed with an atomic reference configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code atomic}, {@code atomic-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the atomic reference resource. If the resource is namespaced - e.g. `references.my-reference.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `references.conf`.
   *
   * @param name The asynchronous atomic reference name.
   * @param uri The asynchronous atomic reference member URI.
   * @param <T> The atomic reference data type.
   * @return The asynchronous atomic reference.
   */
  static <T> AsyncAtomicReference<T> create(String name, String uri) {
    return create(name, uri, new ClusterConfig(), new AsyncAtomicReferenceConfig());
  }

  /**
   * Creates a new asynchronous atomic reference with the default atomic reference configuration.<p>
   *
   * The atomic reference will be constructed with an atomic reference configuration that searches the classpath for three
   * configuration files - {@code {name}}, {@code atomic}, {@code atomic-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the atomic reference resource. If the resource is namespaced - e.g. `references.my-reference.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `references.conf`.
   *
   * @param name The asynchronous atomic reference name.
   * @param uri The asynchronous atomic reference member URI.
   * @param cluster The cluster configuration.
   * @param <T> The atomic reference data type.
   * @return The asynchronous atomic reference.
   */
  static <T> AsyncAtomicReference<T> create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new AsyncAtomicReferenceConfig());
  }

  /**
   * Creates a new asynchronous atomic reference.
   *
   * @param name The asynchronous atomic reference name.
   * @param uri The asynchronous atomic reference member URI.
   * @param cluster The cluster configuration.
   * @param config The atomic reference configuration.
   * @param <T> The atomic reference data type.
   * @return The asynchronous atomic reference.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static <T> AsyncAtomicReference<T> create(String name, String uri, ClusterConfig cluster, AsyncAtomicReferenceConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(uri, new CoordinatorConfig().withName(name).withClusterConfig(cluster));
    AsyncAtomicReference<T> reference = coordinator.getResource(name, config.resolve(cluster));
    ((Resource) reference).addStartupTask(() -> coordinator.open().thenApply(v -> null));
    ((Resource) reference).addShutdownTask(coordinator::close);
    return reference;
  }

}
