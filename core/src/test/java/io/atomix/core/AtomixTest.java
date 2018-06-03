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

import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.Member;
import io.atomix.core.profile.Profile;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.net.Address;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      // Do nothing
    }
    teardownAtomix();
  }

  protected CompletableFuture<Atomix> startAtomix(int id, List<Integer> persistentNodes, Profile... profiles) {
    return startAtomix(id, persistentNodes, b -> b.withProfiles(profiles).build());
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(int id, List<Integer> persistentIds) {
    return startAtomix(id, persistentIds, b -> b.build());
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(int id, List<Integer> persistentIds, Function<Atomix.Builder, Atomix> builderFunction) {
    Atomix atomix = createAtomix(id, persistentIds, builderFunction);
    instances.add(atomix);
    return atomix.start().thenApply(v -> atomix);
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(int id, List<Integer> persistentIds, Map<String, String> metadata, Profile... profiles) {
    Atomix atomix = createAtomix(id, persistentIds, metadata, builder -> builder.withProfiles(profiles).build());
    instances.add(atomix);
    return atomix.start().thenApply(v -> atomix);
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(int id, List<Integer> persistentIds, Map<String, String> metadata, Function<Atomix.Builder, Atomix> builderFunction) {
    Atomix atomix = createAtomix(id, persistentIds, metadata, builderFunction);
    instances.add(atomix);
    return atomix.start().thenApply(v -> atomix);
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUpPersistent() throws Exception {
    Atomix atomix1 = startAtomix(1, Arrays.asList(1), Profile.CONSENSUS).get(30, TimeUnit.SECONDS);
    Atomix atomix2 = startAtomix(2, Arrays.asList(1, 2), Profile.CLIENT).get(30, TimeUnit.SECONDS);
    Atomix atomix3 = startAtomix(3, Arrays.asList(1, 2, 3), Profile.CLIENT).get(30, TimeUnit.SECONDS);
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testBootstrapDataGrid() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>(3);
    futures.add(startAtomix(1, Arrays.asList(), Profile.DATA_GRID));
    futures.add(startAtomix(2, Arrays.asList(1), Profile.DATA_GRID));
    futures.add(startAtomix(3, Arrays.asList(1), Profile.DATA_GRID));
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(30, TimeUnit.SECONDS);
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUpEphemeral() throws Exception {
    Atomix atomix1 = startAtomix(1, Arrays.asList(), Profile.DATA_GRID).get(30, TimeUnit.SECONDS);
    Atomix atomix2 = startAtomix(2, Arrays.asList(1), Profile.DATA_GRID).get(30, TimeUnit.SECONDS);
    Atomix atomix3 = startAtomix(3, Arrays.asList(1), Profile.DATA_GRID).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testDiscoverData() throws Exception {
    Address multicastAddress = Address.from("230.0.0.1", findAvailablePort(1234));
    Atomix atomix1 = startAtomix(1, Arrays.asList(), builder ->
        builder.withProfiles(Profile.DATA_GRID)
            .withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .get(30, TimeUnit.SECONDS);
    Atomix atomix2 = startAtomix(2, Arrays.asList(), builder ->
        builder.withProfiles(Profile.DATA_GRID)
            .withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .get(30, TimeUnit.SECONDS);
    Atomix atomix3 = startAtomix(3, Arrays.asList(), builder ->
        builder.withProfiles(Profile.DATA_GRID)
            .withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .get(30, TimeUnit.SECONDS);

    Thread.sleep(1000);

    assertEquals(3, atomix1.membershipService().getMembers().size());
    assertEquals(3, atomix2.membershipService().getMembers().size());
    assertEquals(3, atomix3.membershipService().getMembers().size());
  }

  @Test
  public void testStopStartConsensus() throws Exception {
    Atomix atomix1 = startAtomix(1, Arrays.asList(1), Profile.CONSENSUS).get(30, TimeUnit.SECONDS);
    atomix1.stop().get(30, TimeUnit.SECONDS);
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
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), Profile.DATA_GRID));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), Profile.DATA_GRID));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), Profile.DATA_GRID));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);
    TestClusterMembershipEventListener eventListener1 = new TestClusterMembershipEventListener();
    instances.get(0).membershipService().addListener(eventListener1);
    TestClusterMembershipEventListener eventListener2 = new TestClusterMembershipEventListener();
    instances.get(1).membershipService().addListener(eventListener2);
    TestClusterMembershipEventListener eventListener3 = new TestClusterMembershipEventListener();
    instances.get(2).membershipService().addListener(eventListener3);
    instances.get(0).stop().get(30, TimeUnit.SECONDS);
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, eventListener2.event().type());
    assertEquals(2, instances.get(1).membershipService().getMembers().size());
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, eventListener3.event().type());
    assertEquals(2, instances.get(2).membershipService().getMembers().size());
    instances.get(1).stop().get(30, TimeUnit.SECONDS);
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, eventListener3.event().type());
    assertEquals(1, instances.get(2).membershipService().getMembers().size());
    instances.get(2).stop().get(30, TimeUnit.SECONDS);
  }

  /**
   * Tests a client joining and leaving the cluster.
   */
  @Test
  public void testClientJoinLeaveDataGrid() throws Exception {
    testClientJoinLeave(Profile.DATA_GRID);
  }

  /**
   * Tests a client joining and leaving the cluster.
   */
  @Test
  public void testClientJoinLeaveConsensus() throws Exception {
    testClientJoinLeave(Profile.CONSENSUS);
  }

  private void testClientJoinLeave(Profile profile) throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), profile));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), profile));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), profile));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);

    TestClusterMembershipEventListener dataListener = new TestClusterMembershipEventListener();
    instances.get(0).membershipService().addListener(dataListener);

    Atomix client1 = startAtomix(4, Arrays.asList(1, 2, 3), Profile.CLIENT).get(30, TimeUnit.SECONDS);
    assertEquals(1, client1.partitionService().getPartitionGroups().size());

    // client1 added to data node
    ClusterMembershipEvent event1 = dataListener.event();
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, event1.type());

    Thread.sleep(1000);

    TestClusterMembershipEventListener clientListener = new TestClusterMembershipEventListener();
    client1.membershipService().addListener(clientListener);

    Atomix client2 = startAtomix(5, Arrays.asList(1, 2, 3), Profile.CLIENT).get(30, TimeUnit.SECONDS);
    assertEquals(1, client2.partitionService().getPartitionGroups().size());

    // client2 added to data node
    ClusterMembershipEvent event2 = dataListener.event();
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, event2.type());

    // client2 added to client node
    event1 = clientListener.event();
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, event1.type());

    client2.stop().get(30, TimeUnit.SECONDS);

    // client2 removed from data node
    event1 = dataListener.event();
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, event1.type());

    // client2 removed from client node
    event1 = clientListener.event();
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, event1.type());
  }

  /**
   * Tests a client metadata.
   */
  @Test
  public void testClientMetadata() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), Profile.CONSENSUS));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), Profile.CONSENSUS));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), Profile.CONSENSUS));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);

    TestClusterMembershipEventListener dataListener = new TestClusterMembershipEventListener();
    instances.get(0).membershipService().addListener(dataListener);

    Atomix client1 = startAtomix(4, Arrays.asList(1, 2, 3), Collections.singletonMap("a-key", "a-value"), Profile.CLIENT).get(30, TimeUnit.SECONDS);
    assertEquals(1, client1.partitionService().getPartitionGroups().size());

    // client1 added to data node
    ClusterMembershipEvent event1 = dataListener.event();
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, event1.type());

    Member member = event1.subject();

    assertNotNull(member.metadata());
    assertEquals(1, member.metadata().size());
    assertEquals("a-value", member.metadata().get("a-key"));
  }

  private static class TestClusterMembershipEventListener implements ClusterMembershipEventListener {
    private final BlockingQueue<ClusterMembershipEvent> queue = new LinkedBlockingQueue<>();

    @Override
    public void onEvent(ClusterMembershipEvent event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public ClusterMembershipEvent event() throws InterruptedException {
      return queue.take();
    }
  }
}
