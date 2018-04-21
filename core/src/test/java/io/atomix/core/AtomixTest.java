/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core;

import io.atomix.cluster.ClusterEvent;
import io.atomix.cluster.ClusterEventListener;
import io.atomix.cluster.Node;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.net.Address;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Atomix test.
 */
public class AtomixTest extends AbstractAtomixTest {
  private List<Atomix> instances;

  @Before
  public void setupInstances() throws Exception {
    setupAtomix();
    instances = new ArrayList<>();
  }

  @After
  public void teardownInstances() throws Exception {
    List<CompletableFuture<Void>> futures = instances.stream().map(Atomix::stop).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
    } catch (Exception e) {
      // Do nothing
    }
    teardownAtomix();
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(Node.Type type, int id, List<Integer> coreIds, List<Integer> bootstrapIds) {
    return startAtomix(type, id, coreIds, bootstrapIds, b -> b.build());
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(Node.Type type, int id, List<Integer> coreIds, List<Integer> bootstrapIds, Function<Atomix.Builder, Atomix> builderFunction) {
    Atomix atomix = createAtomix(type, id, coreIds, bootstrapIds, builderFunction);
    instances.add(atomix);
    return atomix.start();
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUpCore() throws Exception {
    Atomix atomix1 = startAtomix(Node.Type.PERSISTENT, 1, Arrays.asList(1), Arrays.asList()).join();
    Atomix atomix2 = startAtomix(Node.Type.PERSISTENT, 2, Arrays.asList(1, 2), Arrays.asList()).join();
    Atomix atomix3 = startAtomix(Node.Type.PERSISTENT, 3, Arrays.asList(1, 2, 3), Arrays.asList()).join();
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testBootstrapData() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>(3);
    futures.add(startAtomix(Node.Type.EPHEMERAL, 1, Arrays.asList(), Arrays.asList()));
    futures.add(startAtomix(Node.Type.EPHEMERAL, 2, Arrays.asList(), Arrays.asList(1)));
    futures.add(startAtomix(Node.Type.EPHEMERAL, 3, Arrays.asList(), Arrays.asList(1)));
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUpData() throws Exception {
    Atomix atomix1 = startAtomix(Node.Type.EPHEMERAL, 1, Arrays.asList(), Arrays.asList()).join();
    Atomix atomix2 = startAtomix(Node.Type.EPHEMERAL, 2, Arrays.asList(), Arrays.asList(1)).join();
    Atomix atomix3 = startAtomix(Node.Type.EPHEMERAL, 3, Arrays.asList(), Arrays.asList(1)).join();
  }

  @Test
  public void testDiscoverData() throws Exception {
    Address multicastAddress = Address.from("230.0.0.1", findAvailablePort(1234));
    Atomix atomix1 = startAtomix(Node.Type.EPHEMERAL, 1, Arrays.asList(), Arrays.asList(), builder ->
        builder.withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .join();
    Atomix atomix2 = startAtomix(Node.Type.EPHEMERAL, 2, Arrays.asList(), Arrays.asList(), builder ->
        builder.withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .join();
    Atomix atomix3 = startAtomix(Node.Type.EPHEMERAL, 3, Arrays.asList(), Arrays.asList(), builder ->
        builder.withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .join();

    Thread.sleep(1000);

    assertEquals(3, atomix1.clusterService().getNodes().size());
    assertEquals(3, atomix2.clusterService().getNodes().size());
    assertEquals(3, atomix3.clusterService().getNodes().size());
  }

  @Test
  public void testStopStartCore() throws Exception {
    Atomix atomix1 = startAtomix(Node.Type.PERSISTENT, 1, Arrays.asList(1), Arrays.asList()).join();
    atomix1.stop().join();
    try {
      atomix1.start().join();
      fail("Expected CompletionException");
    } catch (CompletionException ex) {
      assertTrue(ex.getCause() instanceof IllegalStateException);
      assertEquals("Atomix instance shutdown", ex.getCause().getMessage());
    }
  }

  /**
   * Tests scaling down a cluster.
   */
  @Test
  public void testScaleDownCore() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(Node.Type.PERSISTENT, 1, Arrays.asList(1, 2, 3), Arrays.asList()));
    futures.add(startAtomix(Node.Type.PERSISTENT, 2, Arrays.asList(1, 2, 3), Arrays.asList()));
    futures.add(startAtomix(Node.Type.PERSISTENT, 3, Arrays.asList(1, 2, 3), Arrays.asList()));
    Futures.allOf(futures).join();
    instances.get(0).stop().join();
    instances.get(1).stop().join();
    instances.get(2).stop().join();
  }

  /**
   * Tests a client joining and leaving the cluster.
   */
  @Test
  public void testClientJoinLeaveCore() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(Node.Type.PERSISTENT, 1, Arrays.asList(1, 2, 3), Arrays.asList()));
    futures.add(startAtomix(Node.Type.PERSISTENT, 2, Arrays.asList(1, 2, 3), Arrays.asList()));
    futures.add(startAtomix(Node.Type.PERSISTENT, 3, Arrays.asList(1, 2, 3), Arrays.asList()));
    Futures.allOf(futures).join();

    TestClusterEventListener dataListener = new TestClusterEventListener();
    instances.get(0).clusterService().addListener(dataListener);

    Atomix client1 = startAtomix(Node.Type.EPHEMERAL, 4, Arrays.asList(1, 2, 3), Arrays.asList()).join();

    // client1 added to data node
    ClusterEvent event1 = dataListener.event();
    assertEquals(ClusterEvent.Type.NODE_ADDED, event1.type());
    event1 = dataListener.event();
    assertEquals(ClusterEvent.Type.NODE_ACTIVATED, event1.type());

    Thread.sleep(1000);

    TestClusterEventListener clientListener = new TestClusterEventListener();
    client1.clusterService().addListener(clientListener);

    Atomix client2 = startAtomix(Node.Type.CLIENT, 5, Arrays.asList(1, 2, 3), Arrays.asList()).join();

    // client2 added to data node
    ClusterEvent event2 = dataListener.event();
    assertEquals(ClusterEvent.Type.NODE_ADDED, event2.type());
    event2 = dataListener.event();
    assertEquals(ClusterEvent.Type.NODE_ACTIVATED, event2.type());

    // client2 added to client node
    event1 = clientListener.event();
    assertEquals(ClusterEvent.Type.NODE_ADDED, event1.type());
    event1 = clientListener.event();
    assertEquals(ClusterEvent.Type.NODE_ACTIVATED, event1.type());

    client2.stop().join();

    // client2 removed from data node
    event1 = dataListener.event();
    assertEquals(ClusterEvent.Type.NODE_DEACTIVATED, event1.type());
    event1 = dataListener.event();
    assertEquals(ClusterEvent.Type.NODE_REMOVED, event1.type());

    // client2 removed from client node
    event1 = clientListener.event();
    assertEquals(ClusterEvent.Type.NODE_DEACTIVATED, event1.type());
    event1 = clientListener.event();
    assertEquals(ClusterEvent.Type.NODE_REMOVED, event1.type());
  }

  private static class TestClusterEventListener implements ClusterEventListener {
    private final BlockingQueue<ClusterEvent> queue = new LinkedBlockingQueue<>();

    @Override
    public void onEvent(ClusterEvent event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public ClusterEvent event() throws InterruptedException {
      return queue.take();
    }
  }
}
