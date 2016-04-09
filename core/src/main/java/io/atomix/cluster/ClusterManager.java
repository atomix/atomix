/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.cluster;

import io.atomix.AtomixReplica;
import io.atomix.copycat.server.cluster.Cluster;

import java.util.concurrent.CompletableFuture;

/**
 * Atomix cluster manager.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ClusterManager {

  /**
   * Cluster manager builder.
   */
  interface Builder extends io.atomix.catalyst.util.Builder<ClusterManager> {
  }

  /**
   * Starts the cluster manager.
   *
   * @param cluster The Copycat cluster.
   * @param replica The Atomix replica.
   * @return A completable future to be completed once the cluster manager has been started.
   */
  CompletableFuture<Void> start(Cluster cluster, AtomixReplica replica);

  /**
   * Stops the cluster manager.
   *
   * @param cluster The Copycat cluster.
   * @param replica The Atomix replica.
   * @return A completable future to be completed once the cluster manager has been stopped.
   */
  CompletableFuture<Void> stop(Cluster cluster, AtomixReplica replica);

}
