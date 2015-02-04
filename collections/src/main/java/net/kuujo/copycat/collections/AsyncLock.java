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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatorConfig;
import net.kuujo.copycat.cluster.internal.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.resource.Resource;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous lock.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncLock extends Resource<AsyncLock> {

  /**
   * Creates a new asynchronous lock with the default cluster configuration.<p>
   *
   * The lock will be constructed with the default cluster configuration. The default cluster configuration
   * searches for two resources on the classpath - {@code cluster} and {cluster-defaults} - in that order. Configuration
   * options specified in {@code cluster.conf} will override those in {cluster-defaults.conf}.<p>
   *
   * Additionally, the lock will be constructed with an lock configuration that searches the classpath for
   * three configuration files - {@code {name}}, {@code lock}, {@code lock-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the lock resource. If the resource is namespaced - e.g. `locks.my-lock.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `locks.conf`.
   *
   * @param name The asynchronous lock name.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name) {
    return create(name, new ClusterConfig(String.format("%s-cluster", name)), new AsyncLockConfig(name));
  }

  /**
   * Creates a new asynchronous lock.<p>
   *
   * The lock will be constructed with an lock configuration that searches the classpath for three configuration
   * files - {@code {name}}, {@code lock}, {@code lock-defaults}, {@code resource}, and
   * {@code resource-defaults} - in that order. The first resource is a configuration resource with the same name
   * as the lock resource. If the resource is namespaced - e.g. `locks.my-lock.conf` - then resource
   * configurations will be loaded according to namespaces as well; for example, `locks.conf`.
   *
   * @param name The asynchronous lock name.
   * @param cluster The cluster configuration.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name, ClusterConfig cluster) {
    return create(name, cluster, new AsyncLockConfig(name));
  }

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @param cluster The cluster configuration.
   * @param config The lock configuration.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name, ClusterConfig cluster, AsyncLockConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(new CoordinatorConfig().withName(name).withClusterConfig(cluster));
    return coordinator.<AsyncLock>getResource(name, config.resolve(cluster))
      .addStartupTask(() -> coordinator.open().thenApply(v -> null))
      .addShutdownTask(coordinator::close);
  }

  /**
   * Acquires the lock.
   *
   * @return A completable future to be completed once the lock has been acquired.
   */
  CompletableFuture<Boolean> lock();

  /**
   * Releases the lock.
   *
   * @return A completable future to be completed once the lock has been released.
   */
  CompletableFuture<Void> unlock();

}
