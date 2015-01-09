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
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.coordinator.CoordinatorConfig;
import net.kuujo.copycat.internal.cluster.coordinator.DefaultClusterCoordinator;

/**
 * Asynchronous set.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The set data type.
 */
public interface AsyncSet<T> extends AsyncCollection<AsyncSet<T>, T>, AsyncSetProxy<T> {

  /**
   * Creates a new asynchronous set.
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
  @SuppressWarnings({"unchecked", "rawtypes"})
  static <T> AsyncSet<T> create(String name, String uri, ClusterConfig cluster, AsyncSetConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(uri, new CoordinatorConfig().withClusterConfig(cluster).addResourceConfig(name, config.resolve(cluster)));
    return coordinator.<AsyncSet<T>>getResource(name)
      .withStartupTask(() -> coordinator.open().thenApply(v -> null))
      .withShutdownTask(coordinator::close);
  }

}
