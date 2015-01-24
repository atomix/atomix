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

import net.kuujo.copycat.resource.Resource;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.internal.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatorConfig;
import net.kuujo.copycat.cluster.internal.coordinator.DefaultClusterCoordinator;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous lock.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncLock extends Resource<AsyncLock> {

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @param uri The asynchronous lock member URI.
   * @param cluster The cluster configuration.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new AsyncLockConfig());
  }

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @param uri The asynchronous lock member URI.
   * @param cluster The cluster configuration.
   * @param config The lock configuration.
   * @return The asynchronous lock.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static AsyncLock create(String name, String uri, ClusterConfig cluster, AsyncLockConfig config) {
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(uri, new CoordinatorConfig().withClusterConfig(cluster));
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
