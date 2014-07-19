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
package net.kuujo.copycat.test.integration;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.vertx.cluster.ClusterManager;
import net.kuujo.copycat.vertx.cluster.LocalClusterManager;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Cluster manager test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterManagerTest extends TestVerticle {

  @Test
  public void testLocalClusterManager() {
    ClusterManager cluster1 = new LocalClusterManager("test", vertx);
    cluster1.start(new Handler<AsyncResult<ClusterConfig>>() {
      @Override
      public void handle(AsyncResult<ClusterConfig> result) {
        assertTrue(result.succeeded());
        final ClusterConfig config1 = result.result();
        ClusterManager cluster2 = new LocalClusterManager("test", vertx);
        cluster2.start(new Handler<AsyncResult<ClusterConfig>>() {
          @Override
          public void handle(AsyncResult<ClusterConfig> result) {
            assertTrue(result.succeeded());
            final ClusterConfig config2 = result.result();
            vertx.runOnContext(new Handler<Void>() {
              @Override
              public void handle(Void event) {
                assertEquals(config1.getLocalMember(), config2.getRemoteMembers().iterator().next());
                assertEquals(config2.getLocalMember(), config1.getRemoteMembers().iterator().next());
                testComplete();
              }
            });
          }
        });
      }
    });
  }

}
