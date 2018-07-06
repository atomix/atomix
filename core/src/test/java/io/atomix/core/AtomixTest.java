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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
  protected CompletableFuture<Atomix> startAtomix(int id, List<Integer> persistentIds, Function<AtomixBuilder, Atomix> builderFunction) {
    Atomix atomix = createAtomix(id, persistentIds, builderFunction);
    instances.add(atomix);
    return atomix.start().thenApply(v -> atomix);
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(int id, List<Integer> persistentIds, Properties properties, Profile... profiles) {
    Atomix atomix = createAtomix(id, persistentIds, properties, builder -> builder.withProfiles(profiles).build());
    instances.add(atomix);
    return atomix.start().thenApply(v -> atomix);
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(int id, List<Integer> persistentIds, Properties properties, Function<AtomixBuilder, Atomix> builderFunction) {
    Atomix atomix = createAtomix(id, persistentIds, properties, builderFunction);
    instances.add(atomix);
    return atomix.start().thenApply(v -> atomix);
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUpPersistent() throws Exception {
    Atomix atomix1 = startAtomix(1, Arrays.asList(1), Profile.consensus("1")).get(30, TimeUnit.SECONDS);
    Atomix atomix2 = startAtomix(2, Arrays.asList(1, 2), Profile.client()).get(30, TimeUnit.SECONDS);
    Atomix atomix3 = startAtomix(3, Arrays.asList(1, 2, 3), Profile.client()).get(30, TimeUnit.SECONDS);
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testBootstrapDataGrid() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>(3);
    futures.add(startAtomix(1, Arrays.asList(), Profile.dataGrid()));
    futures.add(startAtomix(2, Arrays.asList(1), Profile.dataGrid()));
    futures.add(startAtomix(3, Arrays.asList(1), Profile.dataGrid()));
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(30, TimeUnit.SECONDS);
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUpEphemeral() throws Exception {
    Atomix atomix1 = startAtomix(1, Arrays.asList(), Profile.dataGrid()).get(30, TimeUnit.SECONDS);
    Atomix atomix2 = startAtomix(2, Arrays.asList(1), Profile.dataGrid()).get(30, TimeUnit.SECONDS);
    Atomix atomix3 = startAtomix(3, Arrays.asList(1), Profile.dataGrid()).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testDiscoverData() throws Exception {
    Address multicastAddress = Address.from("230.0.0.1", findAvailablePort(1234));
    Atomix atomix1 = startAtomix(1, Arrays.asList(), builder ->
        builder.withProfiles(Profile.dataGrid())
            .withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .get(30, TimeUnit.SECONDS);
    Atomix atomix2 = startAtomix(2, Arrays.asList(), builder ->
        builder.withProfiles(Profile.dataGrid())
            .withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .get(30, TimeUnit.SECONDS);
    Atomix atomix3 = startAtomix(3, Arrays.asList(), builder ->
        builder.withProfiles(Profile.dataGrid())
            .withMulticastEnabled()
            .withMulticastAddress(multicastAddress)
            .build())
        .get(30, TimeUnit.SECONDS);

    Thread.sleep(5000);

    assertEquals(3, atomix1.getMembershipService().getMembers().size());
    assertEquals(3, atomix2.getMembershipService().getMembers().size());
    assertEquals(3, atomix3.getMembershipService().getMembers().size());
  }

  @Test
  public void testStopStartConsensus() throws Exception {
    Atomix atomix1 = startAtomix(1, Arrays.asList(1), Profile.consensus("1")).get(30, TimeUnit.SECONDS);
    atomix1.stop().get(30, TimeUnit.SECONDS);
    try {
      atomix1.start().get(30, TimeUnit.SECONDS);
      fail("Expected ExecutionException");
    } catch (ExecutionException ex) {
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
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), Profile.dataGrid()));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), Profile.dataGrid()));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), Profile.dataGrid()));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);
    TestClusterMembershipEventListener eventListener1 = new TestClusterMembershipEventListener();
    instances.get(0).getMembershipService().addListener(eventListener1);
    TestClusterMembershipEventListener eventListener2 = new TestClusterMembershipEventListener();
    instances.get(1).getMembershipService().addListener(eventListener2);
    TestClusterMembershipEventListener eventListener3 = new TestClusterMembershipEventListener();
    instances.get(2).getMembershipService().addListener(eventListener3);
    instances.get(0).stop().get(30, TimeUnit.SECONDS);
    assertEquals(ClusterMembershipEvent.Type.REACHABILITY_CHANGED, eventListener2.event().type());
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, eventListener2.event().type());
    assertEquals(2, instances.get(1).getMembershipService().getMembers().size());
    assertEquals(ClusterMembershipEvent.Type.REACHABILITY_CHANGED, eventListener3.event().type());
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, eventListener3.event().type());
    assertEquals(2, instances.get(2).getMembershipService().getMembers().size());
    instances.get(1).stop().get(30, TimeUnit.SECONDS);
    assertEquals(ClusterMembershipEvent.Type.REACHABILITY_CHANGED, eventListener3.event().type());
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, eventListener3.event().type());
    assertEquals(1, instances.get(2).getMembershipService().getMembers().size());
    instances.get(2).stop().get(30, TimeUnit.SECONDS);
  }

  /**
   * Tests a client joining and leaving the cluster.
   */
  @Test
  public void testClientJoinLeaveDataGrid() throws Exception {
    testClientJoinLeave(Profile.dataGrid());
  }

  /**
   * Tests a client joining and leaving the cluster.
   */
  @Test
  public void testClientJoinLeaveConsensus() throws Exception {
    testClientJoinLeave(Profile.consensus("1", "2", "3"));
  }

  private void testClientJoinLeave(Profile profile) throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), profile));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), profile));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), profile));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);

    TestClusterMembershipEventListener dataListener = new TestClusterMembershipEventListener();
    instances.get(0).getMembershipService().addListener(dataListener);

    Atomix client1 = startAtomix(4, Arrays.asList(1, 2, 3), Profile.client()).get(30, TimeUnit.SECONDS);
    assertEquals(1, client1.getPartitionService().getPartitionGroups().size());

    // client1 added to data node
    ClusterMembershipEvent event1 = dataListener.event();
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, event1.type());

    Thread.sleep(1000);

    TestClusterMembershipEventListener clientListener = new TestClusterMembershipEventListener();
    client1.getMembershipService().addListener(clientListener);

    Atomix client2 = startAtomix(5, Arrays.asList(1, 2, 3), Profile.client()).get(30, TimeUnit.SECONDS);
    assertEquals(1, client2.getPartitionService().getPartitionGroups().size());

    // client2 added to data node
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, dataListener.event().type());

    // client2 added to client node
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, clientListener.event().type());

    client2.stop().get(30, TimeUnit.SECONDS);

    // client2 removed from data node
    assertEquals(ClusterMembershipEvent.Type.REACHABILITY_CHANGED, dataListener.event().type());
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, dataListener.event().type());

    // client2 removed from client node
    assertEquals(ClusterMembershipEvent.Type.REACHABILITY_CHANGED, clientListener.event().type());
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, clientListener.event().type());
  }

  /**
   * Tests a client properties.
   */
  @Test
  public void testClientProperties() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), Profile.consensus("1", "2", "3")));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), Profile.consensus("1", "2", "3")));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), Profile.consensus("1", "2", "3")));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);

    TestClusterMembershipEventListener dataListener = new TestClusterMembershipEventListener();
    instances.get(0).getMembershipService().addListener(dataListener);

    Properties properties = new Properties();
    properties.setProperty("a-key", "a-value");
    Atomix client1 = startAtomix(4, Arrays.asList(1, 2, 3), properties, Profile.client()).get(30, TimeUnit.SECONDS);
    assertEquals(1, client1.getPartitionService().getPartitionGroups().size());

    // client1 added to data node
    ClusterMembershipEvent event1 = dataListener.event();
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, event1.type());
    event1 = dataListener.event();
    assertEquals(ClusterMembershipEvent.Type.METADATA_CHANGED, event1.type());

    Member member = event1.subject();

    assertNotNull(member.properties());
    assertEquals(1, member.properties().size());
    assertEquals("a-value", member.properties().get("a-key"));
  }

  private static class TestClusterMembershipEventListener implements ClusterMembershipEventListener {
    private final BlockingQueue<ClusterMembershipEvent> queue = new LinkedBlockingQueue<>();

    @Override
    public void event(ClusterMembershipEvent event) {
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
      return queue.poll(10, TimeUnit.SECONDS);
    }
  }
}
