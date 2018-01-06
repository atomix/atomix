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

import com.google.common.base.Throwables;
import io.atomix.cluster.ClusterEvent;
import io.atomix.cluster.ClusterEventListener;
import io.atomix.cluster.Node;
import io.atomix.utils.concurrent.Futures;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Atomix test.
 */
public class AtomixTest extends AbstractAtomixTest {
  private List<Atomix> instances;

  @Before
  public void setupInstances() throws Exception {
    AbstractAtomixTest.setupAtomix();
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
    AbstractAtomixTest.teardownAtomix();
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(Node.Type type, int id, Integer... ids) {
    Atomix atomix = createAtomix(type, id, ids);
    instances.add(atomix);
    return atomix.start();
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUp() throws Exception {
    Atomix atomix1 = startAtomix(Node.Type.DATA, 1, 1).join();
    Atomix atomix2 = startAtomix(Node.Type.DATA, 2, 1, 2).join();
    Atomix atomix3 = startAtomix(Node.Type.DATA, 3, 1, 2, 3).join();
  }

  /**
   * Tests scaling down a cluster.
   */
  @Test
  public void testScaleDown() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(Node.Type.DATA, 1, 1, 2, 3));
    futures.add(startAtomix(Node.Type.DATA, 2, 1, 2, 3));
    futures.add(startAtomix(Node.Type.DATA, 3, 1, 2, 3));
    Futures.allOf(futures).join();
    instances.get(0).stop().join();
    instances.get(1).stop().join();
    instances.get(2).stop().join();
  }

  /**
   * Tests a client joining and leaving the cluster.
   */
  @Test
  public void testClientJoinLeave() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(Node.Type.DATA, 1, 1, 2, 3));
    futures.add(startAtomix(Node.Type.DATA, 2, 1, 2, 3));
    futures.add(startAtomix(Node.Type.DATA, 3, 1, 2, 3));
    Futures.allOf(futures).join();

    TestClusterEventListener dataListener = new TestClusterEventListener();
    instances.get(0).clusterService().addListener(dataListener);

    Atomix client1 = startAtomix(Node.Type.CLIENT, 4, 1, 2, 3).join();

    // client1 added to data node
    ClusterEvent event1 = dataListener.event();
    assertEquals(ClusterEvent.Type.NODE_ADDED, event1.type());
    event1 = dataListener.event();
    assertEquals(ClusterEvent.Type.NODE_ACTIVATED, event1.type());

    Thread.sleep(1000);

    TestClusterEventListener clientListener = new TestClusterEventListener();
    client1.clusterService().addListener(clientListener);

    Atomix client2 = startAtomix(Node.Type.CLIENT, 5, 1, 2, 3).join();

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

    private final BlockingQueue<ClusterEvent> queue = new ArrayBlockingQueue<>(1);

    @Override
    public void onEvent(ClusterEvent event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
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
