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

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.cluster.coordinator.ClusterCoordinator;
import net.kuujo.copycat.internal.cluster.CoordinatedClusterManager;
import net.kuujo.copycat.internal.cluster.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.protocol.LocalProtocol;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Cluster test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ClusterTest extends ConcurrentTestCase {

  /**
   * Tests opening the coordinator.
   */
  public void testCoordinatorOpen() throws Throwable {
    CountDownLatch latch = new CountDownLatch(1);
    Protocol protocol = new LocalProtocol();
    ClusterConfig config = new ClusterConfig()
      .withProtocol(protocol)
      .withMembers("local://foo", "local://bar", "local://baz");
    ClusterCoordinator coordinator = new DefaultClusterCoordinator("local://foo", config, ExecutionContext.create());
    coordinator.open().whenComplete((result, error) -> {
      threadAssertNull(error);
      latch.countDown();
    });
    latch.await(30, TimeUnit.SECONDS);
  }

  /**
   * Tests sending a message between cluster members.
   */
  public void testClusterSendHandle() throws Throwable {
    CountDownLatch latch = new CountDownLatch(1);

    Protocol protocol = new LocalProtocol();

    ExecutionContext context1 = ExecutionContext.create();
    ClusterConfig config1 = new ClusterConfig()
      .withProtocol(protocol)
      .withMembers("local://foo", "local://bar", "local://baz");
    ClusterCoordinator coordinator1 = new DefaultClusterCoordinator("local://foo", config1, ExecutionContext.create());
    ClusterManager cluster1 = new CoordinatedClusterManager(1, coordinator1, context1);

    ExecutionContext context2 = ExecutionContext.create();
    ClusterConfig config2 = new ClusterConfig()
      .withProtocol(protocol)
      .withMembers("local://foo", "local://bar", "local://baz");
    ClusterCoordinator coordinator2 = new DefaultClusterCoordinator("local://bar", config2, ExecutionContext.create());
    ClusterManager cluster2 = new CoordinatedClusterManager(1, coordinator2, context2);

    context1.execute(() -> {
      coordinator1.open().thenRun(() -> {
        cluster1.open().thenRun(() -> {
          context2.execute(() -> {
            coordinator2.open().thenRun(() -> {
              cluster2.open().thenRun(() -> {
                cluster2.member().registerHandler("test", message -> CompletableFuture.completedFuture("world!"));
                context1.execute(() -> {
                  cluster1.member("local://bar").send("test", "Hello").whenComplete((result, error) -> {
                    threadAssertEquals(result, "world!");
                    latch.countDown();
                  });
                });
              });
            });
          });
        });
      });
    });

    latch.await(30, TimeUnit.SECONDS);
  }

}
