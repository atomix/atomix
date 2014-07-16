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
package net.kuujo.copycat.cluster;

import java.lang.reflect.Field;

import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.spi.cluster.ClusterManager;

import com.hazelcast.core.HazelcastInstance;

/**
 * Cluster listener factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ClusterListenerFactory {
  private final Vertx vertx;

  public ClusterListenerFactory(Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Returns the Vert.x Hazelcast instance if one exists.
   *
   * @param vertx The current Vert.x instance.
   * @return The Vert.x Hazelcast instance if Vert.x is clustered.
   */
  static HazelcastInstance getHazelcastInstance(Vertx vertx) {
    VertxInternal vertxInternal = (VertxInternal) vertx;
    ClusterManager clusterManager = vertxInternal.clusterManager();
    if (clusterManager != null) {
      Class<?> clazz = clusterManager.getClass();
      Field field;
      try {
        field = clazz.getDeclaredField("hazelcast");
        field.setAccessible(true);
        return HazelcastInstance.class.cast(field.get(clusterManager));
      } catch (Exception e) {
        return null;
      }
    }
    return null;
  }

  /**
   * Creates a cluster listener.
   *
   * @param cluster The name of the cluster on which to listen.
   * @param localOnly Indicates whether to force the cluster to be local only.
   * @return A new cluster listener.
   */
  public ClusterListener createClusterListener(String cluster, boolean localOnly) {
    if (localOnly) return new LocalClusterListener(cluster, vertx);
    HazelcastInstance hazelcast = getHazelcastInstance(vertx);
    if (hazelcast != null) {
      return new HazelcastClusterListener(cluster, vertx, hazelcast);
    } else {
      return new LocalClusterListener(cluster, vertx);
    }
  }


}
