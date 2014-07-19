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
package net.kuujo.copycat.vertx.cluster;

import net.kuujo.copycat.cluster.ClusterConfig;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * Cluster membership manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ClusterManager {

  /**
   * Returns the underlying cluster configuration.
   *
   * @return The cluster configuration.
   */
  ClusterConfig config();

  /**
   * Starts the cluster manager.
   *
   * @return The cluster manager.
   */
  ClusterManager start();

  /**
   * Starts the cluster manager.
   *
   * @param doneHandler An asynchronous handler to be called once the cluster manager
   *        has been started. Once the manager is started the underlying cluster configuration
   *        will have been updated with the current cluster configuration.
   * @return The cluster manager.
   */
  ClusterManager start(Handler<AsyncResult<ClusterConfig>> doneHandler);

  /**
   * Stops the cluster manager.
   */
  void stop();

  /**
   * Stops the cluster manager.
   *
   * @param doneHandler An asynchronous handler to be called once the cluster manager
   *        has been stopped. Once stopped, the cluster manager will no longer update the
   *        underlying cluster configuration.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
