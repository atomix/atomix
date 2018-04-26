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
import io.atomix.cluster.Member;
import io.atomix.core.profile.Profile;
import io.atomix.core.profile.Profiles;
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

  protected CompletableFuture<Atomix> startAtomix(Member.Type type, int id, List<Integer> persistentNodes, Profile... profiles) {
    return startAtomix(type, id, persistentNodes, Arrays.asList(), profiles);
  }

  protected CompletableFuture<Atomix> startAtomix(Member.Type type, int id, List<Integer> persistentNodes, List<Integer> ephemeralNodes, Profile... profiles) {
    return startAtomix(type, id, persistentNodes, ephemeralNodes, b -> b.withProfiles(profiles).build());
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(Member.Type type, int id, List<Integer> persistentIds, List<Integer> ephemeralIds) {
    return startAtomix(type, id, persistentIds, ephemeralIds, b -> b.build());
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(Member.Type type, int id, List<Integer> persistentIds, List<Integer> ephemeralIds, Function<Atomix.Builder, Atomix> builderFunction) {
    Atomix atomix = createAtomix(type, id, persistentIds, ephemeralIds, builderFunction);
    instances.add(atomix);
    return atomix.start();
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUpPersistent() throws Exception {
    Atomix atomix1 = startAtomix(Member.Type.PERSISTENT, 1, Arrays.asList(1), Arrays.asList(), Profiles.CONSENSUS).join();
    Atomix atomix2 = startAtomix(Member.Type.PERSISTENT, 2, Arrays.asList(1, 2), Arrays.asList(), Profiles.CLIENT).join();
    Atomix atomix3 = startAtomix(Member.Type.PERSISTENT, 3, Arrays.asList(1, 2, 3), Arrays.asList(), Profiles.CLIENT).join();
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testBootstrapEphemeral() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>(3);
    futures.add(startAtomix(Member.Type.EPHEMERAL, 1, Arrays.asList(), Arrays.asList(), Profiles.DATA_GRID));
    futures.add(startAtomix(Member.Type.EPHEMERAL, 2, Arrays.asList(), Arrays.asList(1), Profiles.DATA_GRID));
    futures.add(startAtomix(Member.Type.EPHEMERAL, 3, Arrays.asList(), Arrays.asList(1), Profiles.DATA_GRID));
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUpEphemeral() throws Exception {
    Atomix atomix1 = startAtomix(Member.Type.EPHEMERAL, 1, Arrays.asList(), Arrays.asList(), Profiles.DATA_GRID).join();
    Atomix atomix2 = startAtomix(Member.Type.EPHEMERAL, 2, Arrays.asList(), Arrays.asList(1), Profiles.DATA_GRID).join();
    Atomix atomix3 = startAtomix(Member.Type.EPHEMERAL, 3, Arrays.asList(), Arrays.asList(1), Profiles.DATA_GRID).join();
  }

  @Test
  public void testDiscoverData() throws Exception {
    Address multicastAddress = Address.from("230.0.0.1", findAvailablePort(1234));
    Atomix atomix1 = startAtomix(Member.Type.EPHEMERAL, 1, Arrays.asList(), Arrays.asList(), builder ->
        builder.withProfiles(Profiles.DATA_GRID)
            .withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .join();
    Atomix atomix2 = startAtomix(Member.Type.EPHEMERAL, 2, Arrays.asList(), Arrays.asList(), builder ->
        builder.withProfiles(Profiles.DATA_GRID)
            .withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .join();
    Atomix atomix3 = startAtomix(Member.Type.EPHEMERAL, 3, Arrays.asList(), Arrays.asList(), builder ->
        builder.withProfiles(Profiles.DATA_GRID)
            .withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .join();

    Thread.sleep(1000);

    assertEquals(3, atomix1.membershipService().getMembers().size());
    assertEquals(3, atomix2.membershipService().getMembers().size());
    assertEquals(3, atomix3.membershipService().getMembers().size());
  }

  @Test
  public void testStopStartConsensus() throws Exception {
    Atomix atomix1 = startAtomix(Member.Type.PERSISTENT, 1, Arrays.asList(1), Arrays.asList(), Profiles.CONSENSUS).join();
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
  public void testScaleDownPersistent() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(Member.Type.PERSISTENT, 1, Arrays.asList(1, 2, 3), Arrays.asList(), Profiles.DATA_GRID));
    futures.add(startAtomix(Member.Type.PERSISTENT, 2, Arrays.asList(1, 2, 3), Arrays.asList(), Profiles.DATA_GRID));
    futures.add(startAtomix(Member.Type.PERSISTENT, 3, Arrays.asList(1, 2, 3), Arrays.asList(), Profiles.DATA_GRID));
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
    futures.add(startAtomix(Member.Type.PERSISTENT, 1, Arrays.asList(1, 2, 3), Profiles.CONSENSUS));
    futures.add(startAtomix(Member.Type.PERSISTENT, 2, Arrays.asList(1, 2, 3), Profiles.CONSENSUS));
    futures.add(startAtomix(Member.Type.PERSISTENT, 3, Arrays.asList(1, 2, 3), Profiles.CONSENSUS));
    Futures.allOf(futures).join();

    TestClusterEventListener dataListener = new TestClusterEventListener();
    instances.get(0).membershipService().addListener(dataListener);

    Atomix client1 = startAtomix(Member.Type.EPHEMERAL, 4, Arrays.asList(1, 2, 3), Profiles.CLIENT).join();
    assertEquals(1, client1.partitionService().getPartitionGroups().size());

    // client1 added to data node
    ClusterEvent event1 = dataListener.event();
    assertEquals(ClusterEvent.Type.NODE_ADDED, event1.type());
    event1 = dataListener.event();
    assertEquals(ClusterEvent.Type.NODE_ACTIVATED, event1.type());

    Thread.sleep(1000);

    TestClusterEventListener clientListener = new TestClusterEventListener();
    client1.membershipService().addListener(clientListener);

    Atomix client2 = startAtomix(Member.Type.EPHEMERAL, 5, Arrays.asList(1, 2, 3), Profiles.CLIENT).join();
    assertEquals(1, client2.partitionService().getPartitionGroups().size());

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
