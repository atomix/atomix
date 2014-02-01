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

import java.util.Observable;
import java.util.Observer;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ClusterController;
import net.kuujo.copycat.cluster.impl.DefaultClusterController;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.assertFalse;
import static org.vertx.testtools.VertxAssert.testComplete;

import org.vertx.testtools.TestVerticle;

/**
 * A service test.
 *
 * @author Jordan Halterman
 */
public class ClusterTest extends TestVerticle {

  @Test
  public void testClusterConfigAddMember() {
    ClusterConfig config = new ClusterConfig();
    config.addObserver(new Observer() {
      @Override
      public void update(Observable config, Object arg) {
        assertTrue(((ClusterConfig) config).containsMember("test"));
        testComplete();
      }
    });
    config.addMember("test");
  }

  @Test
  public void testClusterConfigRemoveMember() {
    ClusterConfig config = new ClusterConfig("test");
    config.addObserver(new Observer() {
      @Override
      public void update(Observable config, Object arg) {
        assertFalse(((ClusterConfig) config).containsMember("test"));
        testComplete();
      }
    });
    config.removeMember("test");
  }

  @Test
  public void testClusterControllerLocateMembers() {
    final ClusterController cluster1 = new DefaultClusterController("test.1", "test", vertx);
    cluster1.setBroadcastInterval(1000);
    cluster1.setBroadcastTimeout(500);
    cluster1.start(new Handler<AsyncResult<ClusterConfig>>() {
      @Override
      public void handle(AsyncResult<ClusterConfig> result) {
        assertTrue(result.succeeded());
        final ClusterController cluster2 = new DefaultClusterController("test.2", "test", vertx);
        cluster2.setBroadcastInterval(1000);
        cluster2.setBroadcastTimeout(500);
        cluster2.start(new Handler<AsyncResult<ClusterConfig>>() {
          @Override
          public void handle(AsyncResult<ClusterConfig> result) {
            assertTrue(result.succeeded());
            final ClusterController cluster3 = new DefaultClusterController("test.3", "test", vertx);
            cluster3.setBroadcastInterval(1000);
            cluster3.setBroadcastTimeout(500);
            cluster3.start(new Handler<AsyncResult<ClusterConfig>>() {
              @Override
              public void handle(AsyncResult<ClusterConfig> result) {
                assertTrue(result.succeeded());
                ClusterConfig config = result.result();
                assertTrue(config.containsMember("test.1"));
                assertTrue(config.containsMember("test.2"));
                assertTrue(config.containsMember("test.3"));
                testComplete();
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testClusterControllerAddMember() {
    final ClusterController cluster1 = new DefaultClusterController("test.1", "test", vertx);
    cluster1.setBroadcastInterval(1000);
    cluster1.setBroadcastTimeout(500);
    cluster1.start(new Handler<AsyncResult<ClusterConfig>>() {
      @Override
      public void handle(AsyncResult<ClusterConfig> result) {
        assertTrue(result.succeeded());
        result.result().addObserver(new Observer() {
          @Override
          public void update(Observable config, Object arg) {
            ClusterConfig cluster = (ClusterConfig) config;
            assertTrue(cluster.containsMember("test.2"));
            testComplete();
          }
        });
        final ClusterController cluster2 = new DefaultClusterController("test.2", "test", vertx);
        cluster2.setBroadcastInterval(1000);
        cluster2.setBroadcastTimeout(500);
        cluster2.start();
      }
    });
  }

  @Test
  public void testClusterControllerRemoveMember() {
    final ClusterController cluster1 = new DefaultClusterController("test.1", "test", vertx);
    cluster1.setBroadcastInterval(1000);
    cluster1.setBroadcastTimeout(500);
    cluster1.start(new Handler<AsyncResult<ClusterConfig>>() {
      @Override
      public void handle(AsyncResult<ClusterConfig> result) {
        assertTrue(result.succeeded());
        final ClusterController cluster2 = new DefaultClusterController("test.2", "test", vertx);
        cluster2.setBroadcastInterval(1000);
        cluster2.setBroadcastTimeout(500);
        cluster2.start(new Handler<AsyncResult<ClusterConfig>>() {
          @Override
          public void handle(AsyncResult<ClusterConfig> result) {
            assertTrue(result.succeeded());
            result.result().addObserver(new Observer() {
              @Override
              public void update(Observable config, Object arg) {
                ClusterConfig cluster = (ClusterConfig) config;
                assertFalse(cluster.containsMember("test.1"));
                testComplete();
              }
            });
            cluster1.stop();
          }
        });
      }
    });
  }

}
