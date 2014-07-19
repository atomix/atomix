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

import java.util.UUID;

import net.kuujo.copycat.cluster.DynamicClusterConfig;

import org.vertx.java.core.Vertx;

/**
 * Local cluster manager implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalClusterManager extends ListeningClusterManager {

  public LocalClusterManager(String cluster, Vertx vertx) {
    this(cluster, vertx, 0);
  }

  public LocalClusterManager(String cluster, Vertx vertx, int quorumSize) {
    this(UUID.randomUUID().toString(), cluster, vertx, quorumSize);
  }

  private LocalClusterManager(String address, String cluster, Vertx vertx, int quorumSize) {
    super(address, new DynamicClusterConfig(address, quorumSize), new ClusterListenerFactory(vertx).createClusterListener(cluster, true));
  }

}
