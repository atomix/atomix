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
import io.atomix.core.barrier.DistributedCyclicBarrierType;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.core.counter.DistributedCounterType;
import io.atomix.core.election.LeaderElectionType;
import io.atomix.core.election.LeaderElectorType;
import io.atomix.core.idgenerator.AtomicIdGeneratorType;
import io.atomix.core.list.DistributedListType;
import io.atomix.core.lock.AtomicLockType;
import io.atomix.core.lock.DistributedLockType;
import io.atomix.core.log.DistributedLog;
import io.atomix.core.log.DistributedLogPartition;
import io.atomix.core.map.AtomicCounterMapType;
import io.atomix.core.map.AtomicMapType;
import io.atomix.core.map.AtomicNavigableMapType;
import io.atomix.core.map.AtomicSortedMapType;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapType;
import io.atomix.core.map.DistributedNavigableMapType;
import io.atomix.core.map.DistributedSortedMapType;
import io.atomix.core.multimap.AtomicMultimapType;
import io.atomix.core.multimap.DistributedMultimapType;
import io.atomix.core.multiset.DistributedMultisetType;
import io.atomix.core.profile.ConsensusProfile;
import io.atomix.core.profile.Profile;
import io.atomix.core.queue.DistributedQueueType;
import io.atomix.core.semaphore.AtomicSemaphoreType;
import io.atomix.core.semaphore.DistributedSemaphoreType;
import io.atomix.core.set.DistributedNavigableSetType;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.set.DistributedSortedSetType;
import io.atomix.core.tree.AtomicDocumentTreeType;
import io.atomix.core.value.AtomicValueType;
import io.atomix.core.value.DistributedValueType;
import io.atomix.core.workqueue.WorkQueueType;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.log.partition.LogPartitionGroup;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.config.ConfigurationException;
import io.atomix.utils.net.Address;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

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
    Atomix atomix1 = startAtomix(1, Arrays.asList(1), ConsensusProfile.builder()
        .withMembers("1")
        .withDataPath(new File(DATA_DIR, "scale-up"))
        .build())
        .get(30, TimeUnit.SECONDS);
    Atomix atomix2 = startAtomix(2, Arrays.asList(1, 2), Profile.client()).get(30, TimeUnit.SECONDS);
    Atomix atomix3 = startAtomix(3, Arrays.asList(1, 2, 3), Profile.client()).get(30, TimeUnit.SECONDS);
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testBootstrapDataGrid() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>(3);
    futures.add(startAtomix(1, Arrays.asList(2), Profile.dataGrid()));
    futures.add(startAtomix(2, Arrays.asList(1), Profile.dataGrid()));
    futures.add(startAtomix(3, Arrays.asList(1), Profile.dataGrid()));
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(30, TimeUnit.SECONDS);
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUpEphemeral() throws Exception {
    Atomix atomix1 = startAtomix(1, Arrays.asList(2), Profile.dataGrid()).get(30, TimeUnit.SECONDS);
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
  public void testLogPrimitive() throws Exception {
    CompletableFuture<Atomix> future1 = startAtomix(1, Arrays.asList(1, 2), builder ->
        builder.withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withMembers(String.valueOf(1), String.valueOf(2))
            .withDataDirectory(new File(new File(DATA_DIR, "log"), "1"))
            .build())
            .withPartitionGroups(LogPartitionGroup.builder("log")
                .withNumPartitions(3)
                .build())
            .build());

    CompletableFuture<Atomix> future2 = startAtomix(2, Arrays.asList(1, 2), builder ->
        builder.withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withMembers(String.valueOf(1), String.valueOf(2))
            .withDataDirectory(new File(new File(DATA_DIR, "log"), "2"))
            .build())
            .withPartitionGroups(LogPartitionGroup.builder("log")
                .withNumPartitions(3)
                .build())
            .build());

    Atomix atomix1 = future1.get();
    Atomix atomix2 = future2.get();

    DistributedLog<String> log1 = atomix1.<String>logBuilder()
        .withProtocol(DistributedLogProtocol.builder()
            .build())
        .build();

    DistributedLog<String> log2 = atomix2.<String>logBuilder()
        .withProtocol(DistributedLogProtocol.builder()
            .build())
        .build();

    assertEquals(3, log1.getPartitions().size());
    assertEquals(1, log1.getPartitions().get(0).id());
    assertEquals(2, log1.getPartitions().get(1).id());
    assertEquals(3, log1.getPartitions().get(2).id());
    assertEquals(1, log2.getPartition(1).id());
    assertEquals(2, log2.getPartition(2).id());
    assertEquals(3, log2.getPartition(3).id());

    DistributedLogPartition<String> partition1 = log1.getPartition("Hello world!");
    DistributedLogPartition<String> partition2 = log2.getPartition("Hello world!");
    assertEquals(partition1.id(), partition2.id());

    CountDownLatch latch = new CountDownLatch(2);
    partition2.consume(record -> {
      assertEquals("Hello world!", record.value());
      latch.countDown();
    });
    log2.consume(record -> {
      assertEquals("Hello world!", record.value());
      latch.countDown();
    });
    partition1.produce("Hello world!");
    latch.await(10, TimeUnit.SECONDS);
    assertEquals(0, latch.getCount());
  }

  @Test
  public void testLogBasedPrimitives() throws Exception {
    CompletableFuture<Atomix> future1 = startAtomix(1, Arrays.asList(1, 2), builder ->
        builder.withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withMembers(String.valueOf(1), String.valueOf(2))
            .withDataDirectory(new File(new File(DATA_DIR, "log"), "1"))
            .build())
            .withPartitionGroups(LogPartitionGroup.builder("log")
                .withNumPartitions(3)
                .build())
            .build());

    CompletableFuture<Atomix> future2 = startAtomix(2, Arrays.asList(1, 2), builder ->
        builder.withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withMembers(String.valueOf(1), String.valueOf(2))
            .withDataDirectory(new File(new File(DATA_DIR, "log"), "2"))
            .build())
            .withPartitionGroups(LogPartitionGroup.builder("log")
                .withNumPartitions(3)
                .build())
            .build());

    Atomix atomix1 = future1.get();
    Atomix atomix2 = future2.get();

    DistributedMap<String, String> map1 = atomix1.<String, String>mapBuilder("test-map")
        .withProtocol(DistributedLogProtocol.builder().build())
        .build();

    DistributedMap<String, String> map2 = atomix2.<String, String>mapBuilder("test-map")
        .withProtocol(DistributedLogProtocol.builder().build())
        .build();

    CountDownLatch latch = new CountDownLatch(1);
    map2.addListener(event -> {
      map2.async().get("foo").thenAccept(value -> {
        assertEquals("bar", value);
        latch.countDown();
      });
    });
    map1.put("foo", "bar");
    latch.await(10, TimeUnit.SECONDS);
    assertEquals(0, latch.getCount());

    AtomicCounter counter1 = atomix1.atomicCounterBuilder("test-counter")
        .withProtocol(DistributedLogProtocol.builder().build())
        .build();

    AtomicCounter counter2 = atomix2.atomicCounterBuilder("test-counter")
        .withProtocol(DistributedLogProtocol.builder().build())
        .build();

    assertEquals(1, counter1.incrementAndGet());
    assertEquals(1, counter1.get());
    Thread.sleep(1000);
    assertEquals(1, counter2.get());
    assertEquals(2, counter2.incrementAndGet());
  }

  @Test
  public void testStopStartConsensus() throws Exception {
    Atomix atomix1 = startAtomix(1, Arrays.asList(1), ConsensusProfile.builder()
        .withMembers("1")
        .withDataPath(new File(DATA_DIR, "start-stop-consensus"))
        .build()).get(30, TimeUnit.SECONDS);
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
    testClientJoinLeave(Profile.dataGrid(), Profile.dataGrid(), Profile.dataGrid());
  }

  /**
   * Tests a client joining and leaving the cluster.
   */
  @Test
  public void testClientJoinLeaveConsensus() throws Exception {
    testClientJoinLeave(
        ConsensusProfile.builder()
            .withMembers("1", "2", "3")
            .withDataPath(new File(new File(DATA_DIR, "join-leave"), "1"))
            .build(),
        ConsensusProfile.builder()
            .withMembers("1", "2", "3")
            .withDataPath(new File(new File(DATA_DIR, "join-leave"), "2"))
            .build(),
        ConsensusProfile.builder()
            .withMembers("1", "2", "3")
            .withDataPath(new File(new File(DATA_DIR, "join-leave"), "3"))
            .build());
  }

  private void testClientJoinLeave(Profile... profiles) throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), profiles[0]));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), profiles[1]));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), profiles[2]));
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
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), ConsensusProfile.builder()
        .withMembers("1", "2", "3")
        .withDataPath(new File(new File(DATA_DIR, "client-properties"), "1"))
        .build()));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), ConsensusProfile.builder()
        .withMembers("1", "2", "3")
        .withDataPath(new File(new File(DATA_DIR, "client-properties"), "2"))
        .build()));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), ConsensusProfile.builder()
        .withMembers("1", "2", "3")
        .withDataPath(new File(new File(DATA_DIR, "client-properties"), "3"))
        .build()));
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

    Member member = event1.subject();

    assertNotNull(member.properties());
    assertEquals(1, member.properties().size());
    assertEquals("a-value", member.properties().get("a-key"));
  }

  @Test
  public void testPrimitiveGetters() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), ConsensusProfile.builder()
        .withMembers("1", "2", "3")
        .withDataPath(new File(new File(DATA_DIR, "primitive-getters"), "1"))
        .build()));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), ConsensusProfile.builder()
        .withMembers("1", "2", "3")
        .withDataPath(new File(new File(DATA_DIR, "primitive-getters"), "2"))
        .build()));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), ConsensusProfile.builder()
        .withMembers("1", "2", "3")
        .withDataPath(new File(new File(DATA_DIR, "primitive-getters"), "3"))
        .build()));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);

    Atomix atomix = startAtomix(4, Arrays.asList(1, 2, 3), Profile.client()).get(30, TimeUnit.SECONDS);

    assertEquals("a", atomix.getAtomicCounter("a").name());
    assertEquals(AtomicCounterType.instance(), atomix.getAtomicCounter("a").type());
    assertSame(atomix.getAtomicCounter("a"), atomix.getAtomicCounter("a"));
    assertEquals(1, atomix.getPrimitives(AtomicCounterType.instance()).size());

    assertEquals("b", atomix.getAtomicMap("b").name());
    assertEquals(AtomicMapType.instance(), atomix.getAtomicMap("b").type());
    assertSame(atomix.getAtomicMap("b"), atomix.getAtomicMap("b"));
    assertEquals(2, atomix.getPrimitives(AtomicMapType.instance()).size());

    assertEquals("c", atomix.getAtomicCounterMap("c").name());
    assertEquals(AtomicCounterMapType.instance(), atomix.getAtomicCounterMap("c").type());
    assertSame(atomix.getAtomicCounterMap("c"), atomix.getAtomicCounterMap("c"));
    assertEquals(1, atomix.getPrimitives(AtomicCounterMapType.instance()).size());

    assertEquals("d", atomix.getAtomicDocumentTree("d").name());
    assertEquals(AtomicDocumentTreeType.instance(), atomix.getAtomicDocumentTree("d").type());
    assertSame(atomix.getAtomicDocumentTree("d"), atomix.getAtomicDocumentTree("d"));
    assertEquals(1, atomix.getPrimitives(AtomicDocumentTreeType.instance()).size());

    assertEquals("e", atomix.getAtomicIdGenerator("e").name());
    assertEquals(AtomicIdGeneratorType.instance(), atomix.getAtomicIdGenerator("e").type());
    assertSame(atomix.getAtomicIdGenerator("e"), atomix.getAtomicIdGenerator("e"));
    assertEquals(1, atomix.getPrimitives(AtomicIdGeneratorType.instance()).size());

    assertEquals("f", atomix.getAtomicLock("f").name());
    assertEquals(AtomicLockType.instance(), atomix.getAtomicLock("f").type());
    assertSame(atomix.getAtomicLock("f"), atomix.getAtomicLock("f"));
    assertEquals(1, atomix.getPrimitives(AtomicLockType.instance()).size());

    assertEquals("g", atomix.getAtomicMultimap("g").name());
    assertEquals(AtomicMultimapType.instance(), atomix.getAtomicMultimap("g").type());
    assertSame(atomix.getAtomicMultimap("g"), atomix.getAtomicMultimap("g"));
    assertEquals(1, atomix.getPrimitives(AtomicMultimapType.instance()).size());

    assertEquals("h", atomix.getAtomicNavigableMap("h").name());
    assertEquals(AtomicNavigableMapType.instance(), atomix.getAtomicNavigableMap("h").type());
    assertSame(atomix.getAtomicNavigableMap("h"), atomix.getAtomicNavigableMap("h"));
    assertEquals(1, atomix.getPrimitives(AtomicNavigableMapType.instance()).size());

    assertEquals("i", atomix.getAtomicSemaphore("i").name());
    assertEquals(AtomicSemaphoreType.instance(), atomix.getAtomicSemaphore("i").type());
    assertSame(atomix.getAtomicSemaphore("i"), atomix.getAtomicSemaphore("i"));
    assertEquals(1, atomix.getPrimitives(AtomicSemaphoreType.instance()).size());

    assertEquals("j", atomix.getAtomicSortedMap("j").name());
    assertEquals(AtomicSortedMapType.instance(), atomix.getAtomicSortedMap("j").type());
    assertSame(atomix.getAtomicSortedMap("j"), atomix.getAtomicSortedMap("j"));
    assertEquals(1, atomix.getPrimitives(AtomicSortedMapType.instance()).size());

    assertEquals("k", atomix.getAtomicValue("k").name());
    assertEquals(AtomicValueType.instance(), atomix.getAtomicValue("k").type());
    assertSame(atomix.getAtomicValue("k"), atomix.getAtomicValue("k"));
    assertEquals(1, atomix.getPrimitives(AtomicValueType.instance()).size());

    assertEquals("l", atomix.getCounter("l").name());
    assertEquals(DistributedCounterType.instance(), atomix.getCounter("l").type());
    assertSame(atomix.getCounter("l"), atomix.getCounter("l"));
    assertEquals(1, atomix.getPrimitives(DistributedCounterType.instance()).size());

    assertEquals("m", atomix.getCyclicBarrier("m").name());
    assertEquals(DistributedCyclicBarrierType.instance(), atomix.getCyclicBarrier("m").type());
    assertSame(atomix.getCyclicBarrier("m"), atomix.getCyclicBarrier("m"));
    assertEquals(1, atomix.getPrimitives(DistributedCyclicBarrierType.instance()).size());

    assertEquals("n", atomix.getLeaderElection("n").name());
    assertEquals(LeaderElectionType.instance(), atomix.getLeaderElection("n").type());
    assertSame(atomix.getLeaderElection("n"), atomix.getLeaderElection("n"));
    assertEquals(1, atomix.getPrimitives(LeaderElectionType.instance()).size());

    assertEquals("o", atomix.getLeaderElector("o").name());
    assertEquals(LeaderElectorType.instance(), atomix.getLeaderElector("o").type());
    assertSame(atomix.getLeaderElector("o"), atomix.getLeaderElector("o"));
    assertEquals(1, atomix.getPrimitives(LeaderElectorType.instance()).size());

    assertEquals("p", atomix.getList("p").name());
    assertEquals(DistributedListType.instance(), atomix.getList("p").type());
    assertSame(atomix.getList("p"), atomix.getList("p"));
    assertEquals(1, atomix.getPrimitives(DistributedListType.instance()).size());

    assertEquals("q", atomix.getLock("q").name());
    assertEquals(DistributedLockType.instance(), atomix.getLock("q").type());
    assertSame(atomix.getLock("q"), atomix.getLock("q"));
    assertEquals(1, atomix.getPrimitives(DistributedLockType.instance()).size());

    assertEquals("r", atomix.getMap("r").name());
    assertEquals(DistributedMapType.instance(), atomix.getMap("r").type());
    assertSame(atomix.getMap("r"), atomix.getMap("r"));
    assertEquals(1, atomix.getPrimitives(DistributedMapType.instance()).size());

    assertEquals("s", atomix.getMultimap("s").name());
    assertEquals(DistributedMultimapType.instance(), atomix.getMultimap("s").type());
    assertSame(atomix.getMultimap("s"), atomix.getMultimap("s"));
    assertEquals(1, atomix.getPrimitives(DistributedMultimapType.instance()).size());

    assertEquals("t", atomix.getMultiset("t").name());
    assertEquals(DistributedMultisetType.instance(), atomix.getMultiset("t").type());
    assertSame(atomix.getMultiset("t"), atomix.getMultiset("t"));
    assertEquals(1, atomix.getPrimitives(DistributedMultisetType.instance()).size());

    assertEquals("u", atomix.getNavigableMap("u").name());
    assertEquals(DistributedNavigableMapType.instance(), atomix.getNavigableMap("u").type());
    assertSame(atomix.getNavigableMap("u"), atomix.getNavigableMap("u"));
    assertEquals(1, atomix.getPrimitives(DistributedNavigableMapType.instance()).size());

    assertEquals("v", atomix.getNavigableSet("v").name());
    assertEquals(DistributedNavigableSetType.instance(), atomix.getNavigableSet("v").type());
    assertSame(atomix.getNavigableSet("v"), atomix.getNavigableSet("v"));
    assertEquals(1, atomix.getPrimitives(DistributedNavigableSetType.instance()).size());

    assertEquals("w", atomix.getQueue("w").name());
    assertEquals(DistributedQueueType.instance(), atomix.getQueue("w").type());
    assertSame(atomix.getQueue("w"), atomix.getQueue("w"));
    assertEquals(1, atomix.getPrimitives(DistributedQueueType.instance()).size());

    assertEquals("x", atomix.getSemaphore("x").name());
    assertEquals(DistributedSemaphoreType.instance(), atomix.getSemaphore("x").type());
    assertSame(atomix.getSemaphore("x"), atomix.getSemaphore("x"));
    assertEquals(1, atomix.getPrimitives(DistributedSemaphoreType.instance()).size());

    assertEquals("y", atomix.getSet("y").name());
    assertEquals(DistributedSetType.instance(), atomix.getSet("y").type());
    assertSame(atomix.getSet("y"), atomix.getSet("y"));
    assertEquals(1, atomix.getPrimitives(DistributedSetType.instance()).size());

    assertEquals("z", atomix.getSortedMap("z").name());
    assertEquals(DistributedSortedMapType.instance(), atomix.getSortedMap("z").type());
    assertSame(atomix.getSortedMap("z"), atomix.getSortedMap("z"));
    assertEquals(1, atomix.getPrimitives(DistributedSortedMapType.instance()).size());

    assertEquals("aa", atomix.getSortedSet("aa").name());
    assertEquals(DistributedSortedSetType.instance(), atomix.getSortedSet("aa").type());
    assertSame(atomix.getSortedSet("aa"), atomix.getSortedSet("aa"));
    assertEquals(1, atomix.getPrimitives(DistributedSortedSetType.instance()).size());

    assertEquals("bb", atomix.getValue("bb").name());
    assertEquals(DistributedValueType.instance(), atomix.getValue("bb").type());
    assertSame(atomix.getValue("bb"), atomix.getValue("bb"));
    assertEquals(1, atomix.getPrimitives(DistributedValueType.instance()).size());

    assertEquals("cc", atomix.getWorkQueue("cc").name());
    assertEquals(WorkQueueType.instance(), atomix.getWorkQueue("cc").type());
    assertSame(atomix.getWorkQueue("cc"), atomix.getWorkQueue("cc"));
    assertEquals(1, atomix.getPrimitives(WorkQueueType.instance()).size());

    assertEquals(30, atomix.getPrimitives().size());
  }

  @Test
  public void testPrimitiveConfigurations() throws Exception {
    IntStream.range(1, 4).forEach(i ->
        instances.add(Atomix.builder(getClass().getResource("/primitives.conf").getFile())
            .withMemberId(String.valueOf(i))
            .withHost("localhost")
            .withPort(5000 + i)
            .withProfiles(ConsensusProfile.builder()
                .withMembers("1", "2", "3")
                .withDataPath(new File(new File(DATA_DIR, "primitive-getters"), String.valueOf(i)))
                .build())
            .build()));
    Futures.allOf(instances.stream().map(Atomix::start)).get(30, TimeUnit.SECONDS);

    Atomix atomix = Atomix.builder(getClass().getResource("/primitives.conf").getFile())
        .withHost("localhost")
        .withPort(5000)
        .build();
    instances.add(atomix);

    //try {
    //  atomix.getAtomicCounter("foo");
    //  fail();
    //} catch (IllegalStateException e) {
    //}

    atomix.start().get(30, TimeUnit.SECONDS);

    try {
      atomix.mapBuilder("foo")
          .withProtocol(MultiRaftProtocol.builder("wrong").build())
          .get();
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof ConfigurationException);
    }

    try {
      atomix.mapBuilder("bar")
          .withProtocol(MultiRaftProtocol.builder("wrong").build())
          .build();
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof ConfigurationException);
    }

    assertEquals(AtomicCounterType.instance(), atomix.getPrimitive("atomic-counter", AtomicCounterType.instance()).type());
    assertEquals("atomic-counter", atomix.getAtomicCounter("atomic-counter").name());
    assertEquals("two", ((ProxyProtocol) atomix.getAtomicCounter("atomic-counter").protocol()).group());

    assertEquals(AtomicMapType.instance(), atomix.getPrimitive("atomic-map", AtomicMapType.instance()).type());
    assertEquals("atomic-map", atomix.getAtomicMap("atomic-map").name());
    assertEquals("two", ((ProxyProtocol) atomix.getAtomicMap("atomic-map").protocol()).group());

    assertEquals(AtomicCounterMapType.instance(), atomix.getPrimitive("atomic-counter-map", AtomicCounterMapType.instance()).type());
    assertEquals("atomic-counter-map", atomix.getAtomicCounterMap("atomic-counter-map").name());
    assertEquals("two", ((ProxyProtocol) atomix.getAtomicCounterMap("atomic-counter-map").protocol()).group());

    assertEquals(AtomicDocumentTreeType.instance(), atomix.getPrimitive("atomic-document-tree", AtomicDocumentTreeType.instance()).type());
    assertEquals("atomic-document-tree", atomix.getAtomicDocumentTree("atomic-document-tree").name());
    assertEquals("two", ((ProxyProtocol) atomix.getAtomicDocumentTree("atomic-document-tree").protocol()).group());

    assertEquals(AtomicIdGeneratorType.instance(), atomix.getPrimitive("atomic-id-generator", AtomicIdGeneratorType.instance()).type());
    assertEquals("atomic-id-generator", atomix.getAtomicIdGenerator("atomic-id-generator").name());
    assertEquals("two", ((ProxyProtocol) atomix.getAtomicIdGenerator("atomic-id-generator").protocol()).group());

    assertEquals(AtomicLockType.instance(), atomix.getPrimitive("atomic-lock", AtomicLockType.instance()).type());
    assertEquals("atomic-lock", atomix.getAtomicLock("atomic-lock").name());
    assertEquals("two", ((ProxyProtocol) atomix.getAtomicLock("atomic-lock").protocol()).group());

    assertEquals(AtomicMultimapType.instance(), atomix.getPrimitive("atomic-multimap", AtomicMultimapType.instance()).type());
    assertEquals("atomic-multimap", atomix.getAtomicMultimap("atomic-multimap").name());
    assertEquals("two", ((ProxyProtocol) atomix.getAtomicMultimap("atomic-multimap").protocol()).group());

    assertEquals(AtomicNavigableMapType.instance(), atomix.getPrimitive("atomic-navigable-map", AtomicNavigableMapType.instance()).type());
    assertEquals("atomic-navigable-map", atomix.getAtomicNavigableMap("atomic-navigable-map").name());
    assertEquals("two", ((ProxyProtocol) atomix.getAtomicNavigableMap("atomic-navigable-map").protocol()).group());

    assertEquals(AtomicSemaphoreType.instance(), atomix.getPrimitive("atomic-semaphore", AtomicSemaphoreType.instance()).type());
    assertEquals("atomic-semaphore", atomix.getAtomicSemaphore("atomic-semaphore").name());
    assertEquals("two", ((ProxyProtocol) atomix.getAtomicSemaphore("atomic-semaphore").protocol()).group());

    assertEquals(AtomicSortedMapType.instance(), atomix.getPrimitive("atomic-sorted-map", AtomicSortedMapType.instance()).type());
    assertEquals("atomic-sorted-map", atomix.getAtomicSortedMap("atomic-sorted-map").name());
    assertEquals("two", ((ProxyProtocol) atomix.getAtomicSortedMap("atomic-sorted-map").protocol()).group());

    assertEquals(AtomicValueType.instance(), atomix.getPrimitive("atomic-value", AtomicValueType.instance()).type());
    assertEquals("atomic-value", atomix.getAtomicValue("atomic-value").name());
    assertEquals("two", ((ProxyProtocol) atomix.getAtomicValue("atomic-value").protocol()).group());

    assertEquals(DistributedCounterType.instance(), atomix.getPrimitive("counter", DistributedCounterType.instance()).type());
    assertEquals("counter", atomix.getCounter("counter").name());
    assertEquals("two", ((ProxyProtocol) atomix.getCounter("counter").protocol()).group());

    assertEquals(DistributedCyclicBarrierType.instance(), atomix.getPrimitive("cyclic-barrier", DistributedCyclicBarrierType.instance()).type());
    assertEquals("cyclic-barrier", atomix.getCyclicBarrier("cyclic-barrier").name());
    assertEquals("two", ((ProxyProtocol) atomix.getCyclicBarrier("cyclic-barrier").protocol()).group());

    assertEquals(LeaderElectionType.instance(), atomix.getPrimitive("leader-election", LeaderElectionType.instance()).type());
    assertEquals("leader-election", atomix.getLeaderElection("leader-election").name());
    assertEquals("two", ((ProxyProtocol) atomix.getLeaderElection("leader-election").protocol()).group());

    assertEquals(LeaderElectorType.instance(), atomix.getPrimitive("leader-elector", LeaderElectorType.instance()).type());
    assertEquals("leader-elector", atomix.getLeaderElector("leader-elector").name());
    assertEquals("two", ((ProxyProtocol) atomix.getLeaderElector("leader-elector").protocol()).group());

    assertEquals(DistributedListType.instance(), atomix.getPrimitive("list", DistributedListType.instance()).type());
    assertEquals("list", atomix.getList("list").name());
    assertEquals("two", ((ProxyProtocol) atomix.getList("list").protocol()).group());

    assertEquals(DistributedLockType.instance(), atomix.getPrimitive("lock", DistributedLockType.instance()).type());
    assertEquals("lock", atomix.getLock("lock").name());
    assertEquals("two", ((ProxyProtocol) atomix.getLock("lock").protocol()).group());

    assertEquals(DistributedMapType.instance(), atomix.getPrimitive("map", DistributedMapType.instance()).type());
    assertEquals("map", atomix.getMap("map").name());
    assertEquals("two", ((ProxyProtocol) atomix.getMap("map").protocol()).group());

    assertEquals(DistributedMultimapType.instance(), atomix.getPrimitive("multimap", DistributedMultimapType.instance()).type());
    assertEquals("multimap", atomix.getMultimap("multimap").name());
    assertEquals("two", ((ProxyProtocol) atomix.getMultimap("multimap").protocol()).group());

    assertEquals(DistributedMultisetType.instance(), atomix.getPrimitive("multiset", DistributedMultisetType.instance()).type());
    assertEquals("multiset", atomix.getMultiset("multiset").name());
    assertEquals("two", ((ProxyProtocol) atomix.getMultiset("multiset").protocol()).group());

    assertEquals(DistributedNavigableMapType.instance(), atomix.getPrimitive("navigable-map", DistributedNavigableMapType.instance()).type());
    assertEquals("navigable-map", atomix.getNavigableMap("navigable-map").name());
    assertEquals("two", ((ProxyProtocol) atomix.getNavigableMap("navigable-map").protocol()).group());

    assertEquals(DistributedNavigableSetType.instance(), atomix.getPrimitive("navigable-set", DistributedNavigableSetType.instance()).type());
    assertEquals("navigable-set", atomix.getNavigableSet("navigable-set").name());
    assertEquals("two", ((ProxyProtocol) atomix.getNavigableSet("navigable-set").protocol()).group());

    assertEquals(DistributedQueueType.instance(), atomix.getPrimitive("queue", DistributedQueueType.instance()).type());
    assertEquals("queue", atomix.getQueue("queue").name());
    assertEquals("two", ((ProxyProtocol) atomix.getQueue("queue").protocol()).group());

    assertEquals(DistributedSemaphoreType.instance(), atomix.getPrimitive("semaphore", DistributedSemaphoreType.instance()).type());
    assertEquals("semaphore", atomix.getSemaphore("semaphore").name());
    assertEquals("two", ((ProxyProtocol) atomix.getSemaphore("semaphore").protocol()).group());

    assertEquals(DistributedSetType.instance(), atomix.getPrimitive("set", DistributedSetType.instance()).type());
    assertEquals("set", atomix.getSet("set").name());
    assertEquals("two", ((ProxyProtocol) atomix.getSet("set").protocol()).group());

    assertEquals(DistributedSortedMapType.instance(), atomix.getPrimitive("sorted-map", DistributedSortedMapType.instance()).type());
    assertEquals("sorted-map", atomix.getSortedMap("sorted-map").name());
    assertEquals("two", ((ProxyProtocol) atomix.getSortedMap("sorted-map").protocol()).group());

    assertEquals(DistributedSortedSetType.instance(), atomix.getPrimitive("sorted-set", DistributedSortedSetType.instance()).type());
    assertEquals("sorted-set", atomix.getSortedSet("sorted-set").name());
    assertEquals("two", ((ProxyProtocol) atomix.getSortedSet("sorted-set").protocol()).group());

    assertEquals(DistributedValueType.instance(), atomix.getPrimitive("value", DistributedValueType.instance()).type());
    assertEquals("value", atomix.getValue("value").name());
    assertEquals("two", ((ProxyProtocol) atomix.getValue("value").protocol()).group());

    assertEquals(WorkQueueType.instance(), atomix.getPrimitive("work-queue", WorkQueueType.instance()).type());
    assertEquals("work-queue", atomix.getWorkQueue("work-queue").name());
    assertEquals("two", ((ProxyProtocol) atomix.getWorkQueue("work-queue").protocol()).group());
  }

  @Test
  public void testPrimitiveBuilders() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), ConsensusProfile.builder()
        .withMembers("1", "2", "3")
        .withDataPath(new File(new File(DATA_DIR, "primitive-builders"), "1"))
        .build()));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), ConsensusProfile.builder()
        .withMembers("1", "2", "3")
        .withDataPath(new File(new File(DATA_DIR, "primitive-builders"), "2"))
        .build()));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), ConsensusProfile.builder()
        .withMembers("1", "2", "3")
        .withDataPath(new File(new File(DATA_DIR, "primitive-builders"), "3"))
        .build()));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);

    Atomix atomix = startAtomix(4, Arrays.asList(1, 2, 3), Profile.client()).get(30, TimeUnit.SECONDS);

    assertEquals("a", atomix.atomicCounterBuilder("a").build().name());
    assertEquals(AtomicCounterType.instance(), atomix.atomicCounterBuilder("a").build().type());

    assertEquals("b", atomix.atomicMapBuilder("b").build().name());
    assertEquals(AtomicMapType.instance(), atomix.atomicMapBuilder("b").build().type());

    assertEquals("c", atomix.atomicCounterMapBuilder("c").build().name());
    assertEquals(AtomicCounterMapType.instance(), atomix.atomicCounterMapBuilder("c").build().type());

    assertEquals("d", atomix.atomicDocumentTreeBuilder("d").build().name());
    assertEquals(AtomicDocumentTreeType.instance(), atomix.atomicDocumentTreeBuilder("d").build().type());

    assertEquals("e", atomix.atomicIdGeneratorBuilder("e").build().name());
    assertEquals(AtomicIdGeneratorType.instance(), atomix.atomicIdGeneratorBuilder("e").build().type());

    assertEquals("f", atomix.atomicLockBuilder("f").build().name());
    assertEquals(AtomicLockType.instance(), atomix.atomicLockBuilder("f").build().type());

    assertEquals("g", atomix.atomicMultimapBuilder("g").build().name());
    assertEquals(AtomicMultimapType.instance(), atomix.atomicMultimapBuilder("g").build().type());

    assertEquals("h", atomix.atomicNavigableMapBuilder("h").build().name());
    assertEquals(AtomicNavigableMapType.instance(), atomix.atomicNavigableMapBuilder("h").build().type());

    assertEquals("i", atomix.atomicSemaphoreBuilder("i").build().name());
    assertEquals(AtomicSemaphoreType.instance(), atomix.atomicSemaphoreBuilder("i").build().type());

    assertEquals("j", atomix.atomicSortedMapBuilder("j").build().name());
    assertEquals(AtomicSortedMapType.instance(), atomix.atomicSortedMapBuilder("j").build().type());

    assertEquals("k", atomix.atomicValueBuilder("k").build().name());
    assertEquals(AtomicValueType.instance(), atomix.atomicValueBuilder("k").build().type());

    assertEquals("l", atomix.counterBuilder("l").build().name());
    assertEquals(DistributedCounterType.instance(), atomix.counterBuilder("l").build().type());

    assertEquals("m", atomix.cyclicBarrierBuilder("m").build().name());
    assertEquals(DistributedCyclicBarrierType.instance(), atomix.cyclicBarrierBuilder("m").build().type());

    assertEquals("n", atomix.leaderElectionBuilder("n").build().name());
    assertEquals(LeaderElectionType.instance(), atomix.leaderElectionBuilder("n").build().type());

    assertEquals("o", atomix.leaderElectorBuilder("o").build().name());
    assertEquals(LeaderElectorType.instance(), atomix.leaderElectorBuilder("o").build().type());

    assertEquals("p", atomix.listBuilder("p").build().name());
    assertEquals(DistributedListType.instance(), atomix.listBuilder("p").build().type());

    assertEquals("q", atomix.lockBuilder("q").build().name());
    assertEquals(DistributedLockType.instance(), atomix.lockBuilder("q").build().type());

    assertEquals("r", atomix.mapBuilder("r").build().name());
    assertEquals(DistributedMapType.instance(), atomix.mapBuilder("r").build().type());

    assertEquals("s", atomix.multimapBuilder("s").build().name());
    assertEquals(DistributedMultimapType.instance(), atomix.multimapBuilder("s").build().type());

    assertEquals("t", atomix.multisetBuilder("t").build().name());
    assertEquals(DistributedMultisetType.instance(), atomix.multisetBuilder("t").build().type());

    assertEquals("u", atomix.navigableMapBuilder("u").build().name());
    assertEquals(DistributedNavigableMapType.instance(), atomix.navigableMapBuilder("u").build().type());

    assertEquals("v", atomix.navigableSetBuilder("v").build().name());
    assertEquals(DistributedNavigableSetType.instance(), atomix.navigableSetBuilder("v").build().type());

    assertEquals("w", atomix.queueBuilder("w").build().name());
    assertEquals(DistributedQueueType.instance(), atomix.queueBuilder("w").build().type());

    assertEquals("x", atomix.semaphoreBuilder("x").build().name());
    assertEquals(DistributedSemaphoreType.instance(), atomix.semaphoreBuilder("x").build().type());

    assertEquals("y", atomix.setBuilder("y").build().name());
    assertEquals(DistributedSetType.instance(), atomix.setBuilder("y").build().type());

    assertEquals("z", atomix.sortedMapBuilder("z").build().name());
    assertEquals(DistributedSortedMapType.instance(), atomix.sortedMapBuilder("z").build().type());

    assertEquals("aa", atomix.sortedSetBuilder("aa").build().name());
    assertEquals(DistributedSortedSetType.instance(), atomix.sortedSetBuilder("aa").build().type());

    assertEquals("bb", atomix.valueBuilder("bb").build().name());
    assertEquals(DistributedValueType.instance(), atomix.valueBuilder("bb").build().type());

    assertEquals("cc", atomix.workQueueBuilder("cc").build().name());
    assertEquals(WorkQueueType.instance(), atomix.workQueueBuilder("cc").build().type());
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

    public ClusterMembershipEvent event() throws InterruptedException, TimeoutException {
      ClusterMembershipEvent event = queue.poll(15, TimeUnit.SECONDS);
      if (event == null) {
        throw new TimeoutException();
      }
      return event;
    }
  }
}
