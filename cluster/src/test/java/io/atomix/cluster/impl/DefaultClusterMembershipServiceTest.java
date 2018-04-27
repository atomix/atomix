/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.cluster.impl;

import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.GroupMembershipConfig;
import io.atomix.cluster.ManagedClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.Member.State;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.impl.TestBroadcastServiceFactory;
import io.atomix.cluster.messaging.impl.TestMessagingServiceFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Default cluster service test.
 */
public class DefaultClusterMembershipServiceTest {

  private Member buildNode(int memberId, Member.Type type) {
    return Member.builder(String.valueOf(memberId))
        .withType(type)
        .withAddress("localhost", memberId)
        .build();
  }

  private ClusterMetadata buildClusterMetadata(Integer... bootstrapNodes) {
    List<Member> bootstrap = new ArrayList<>();
    for (int bootstrapNode : bootstrapNodes) {
      bootstrap.add(Member.builder(String.valueOf(bootstrapNode))
          .withType(Member.Type.PERSISTENT)
          .withAddress("localhost", bootstrapNode)
          .build());
    }
    return ClusterMetadata.builder().withNodes(bootstrap).build();
  }

  @Test
  public void testClusterService() throws Exception {
    TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();
    TestBroadcastServiceFactory broadcastServiceFactory = new TestBroadcastServiceFactory();

    ClusterMetadata clusterMetadata = buildClusterMetadata(1, 2, 3);

    Member localMember1 = buildNode(1, Member.Type.PERSISTENT);
    ManagedClusterMembershipService clusterService1 = new DefaultClusterMembershipService(
        localMember1,
        new DefaultBootstrapMetadataService(new ClusterMetadata(Collections.emptyList())),
        new TestPersistentMetadataService(clusterMetadata),
        messagingServiceFactory.newMessagingService(localMember1.address()).start().join(),
        broadcastServiceFactory.newBroadcastService().start().join(),
        new GroupMembershipConfig());

    Member localMember2 = buildNode(2, Member.Type.PERSISTENT);
    ManagedClusterMembershipService clusterService2 = new DefaultClusterMembershipService(
        localMember2,
        new DefaultBootstrapMetadataService(new ClusterMetadata(Collections.emptyList())),
        new TestPersistentMetadataService(clusterMetadata),
        messagingServiceFactory.newMessagingService(localMember2.address()).start().join(),
        broadcastServiceFactory.newBroadcastService().start().join(),
        new GroupMembershipConfig());

    Member localMember3 = buildNode(3, Member.Type.PERSISTENT);
    ManagedClusterMembershipService clusterService3 = new DefaultClusterMembershipService(
        localMember3,
        new DefaultBootstrapMetadataService(new ClusterMetadata(Collections.emptyList())),
        new TestPersistentMetadataService(clusterMetadata),
        messagingServiceFactory.newMessagingService(localMember3.address()).start().join(),
        broadcastServiceFactory.newBroadcastService().start().join(),
        new GroupMembershipConfig());

    assertNull(clusterService1.getMember(MemberId.from("1")));
    assertNull(clusterService1.getMember(MemberId.from("2")));
    assertNull(clusterService1.getMember(MemberId.from("3")));

    CompletableFuture<ClusterMembershipService>[] futures = new CompletableFuture[3];
    futures[0] = clusterService1.start();
    futures[1] = clusterService2.start();
    futures[2] = clusterService3.start();

    CompletableFuture.allOf(futures).join();

    Thread.sleep(1000);

    assertEquals(3, clusterService1.getMembers().size());
    assertEquals(3, clusterService2.getMembers().size());
    assertEquals(3, clusterService3.getMembers().size());

    assertEquals(Member.Type.PERSISTENT, clusterService1.getLocalMember().type());
    assertEquals(Member.Type.PERSISTENT, clusterService1.getMember(MemberId.from("1")).type());
    assertEquals(Member.Type.PERSISTENT, clusterService1.getMember(MemberId.from("2")).type());
    assertEquals(Member.Type.PERSISTENT, clusterService1.getMember(MemberId.from("3")).type());

    assertEquals(State.ACTIVE, clusterService1.getLocalMember().getState());
    assertEquals(State.ACTIVE, clusterService1.getMember(MemberId.from("1")).getState());
    assertEquals(State.ACTIVE, clusterService1.getMember(MemberId.from("2")).getState());
    assertEquals(State.ACTIVE, clusterService1.getMember(MemberId.from("3")).getState());

    Member ephemeralMember = buildNode(4, Member.Type.EPHEMERAL);

    ManagedClusterMembershipService ephemeralClusterService = new DefaultClusterMembershipService(
        ephemeralMember,
        new DefaultBootstrapMetadataService(new ClusterMetadata(Collections.emptyList())),
        new TestPersistentMetadataService(clusterMetadata),
        messagingServiceFactory.newMessagingService(ephemeralMember.address()).start().join(),
        broadcastServiceFactory.newBroadcastService().start().join(),
        new GroupMembershipConfig());

    assertEquals(State.INACTIVE, ephemeralClusterService.getLocalMember().getState());

    assertNull(ephemeralClusterService.getMember(MemberId.from("1")));
    assertNull(ephemeralClusterService.getMember(MemberId.from("2")));
    assertNull(ephemeralClusterService.getMember(MemberId.from("3")));
    assertNull(ephemeralClusterService.getMember(MemberId.from("4")));
    assertNull(ephemeralClusterService.getMember(MemberId.from("5")));

    ephemeralClusterService.start().join();

    Thread.sleep(1000);

    assertEquals(4, clusterService1.getMembers().size());
    assertEquals(4, clusterService2.getMembers().size());
    assertEquals(4, clusterService3.getMembers().size());
    assertEquals(4, ephemeralClusterService.getMembers().size());

    clusterService1.stop().join();

    Thread.sleep(15000);

    assertEquals(4, clusterService2.getMembers().size());
    assertEquals(Member.Type.PERSISTENT, clusterService2.getMember(MemberId.from("1")).type());

    assertEquals(State.INACTIVE, clusterService2.getMember(MemberId.from("1")).getState());
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.from("2")).getState());
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.from("3")).getState());
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.from("4")).getState());

    ephemeralClusterService.stop().join();

    Thread.sleep(15000);

    assertEquals(3, clusterService2.getMembers().size());
    assertEquals(State.INACTIVE, clusterService2.getMember(MemberId.from("1")).getState());
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.from("2")).getState());
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.from("3")).getState());
    assertNull(clusterService2.getMember(MemberId.from("4")));

    Thread.sleep(2500);

    assertEquals(3, clusterService2.getMembers().size());

    assertEquals(State.INACTIVE, clusterService2.getMember(MemberId.from("1")).getState());
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.from("2")).getState());
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.from("3")).getState());
    assertNull(clusterService2.getMember(MemberId.from("4")));
  }
}
