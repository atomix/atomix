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

import io.atomix.cluster.GroupMembershipConfig;
import io.atomix.cluster.ManagedClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.Member.State;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.impl.TestBroadcastServiceFactory;
import io.atomix.cluster.messaging.impl.TestMessagingServiceFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Default cluster service test.
 */
public class DefaultClusterMembershipServiceTest {

  private Member buildMember(int memberId) {
    return Member.builder(String.valueOf(memberId))
        .withAddress("localhost", memberId)
        .build();
  }

  private Collection<Member> buildBootstrapMembers(Integer... bootstrapNodes) {
    List<Member> bootstrap = new ArrayList<>(bootstrapNodes.length);
    for (int bootstrapNode : bootstrapNodes) {
      bootstrap.add(Member.builder(String.valueOf(bootstrapNode))
          .withAddress("localhost", bootstrapNode)
          .build());
    }
    return bootstrap;
  }

  @Test
  public void testClusterService() throws Exception {
    TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();
    TestBroadcastServiceFactory broadcastServiceFactory = new TestBroadcastServiceFactory();

    Collection<Member> bootstrapNodes = buildBootstrapMembers(1, 2, 3);

    Member localMember1 = buildMember(1);
    ManagedClusterMembershipService clusterService1 = new DefaultClusterMembershipService(
        localMember1,
        bootstrapNodes,
        messagingServiceFactory.newMessagingService(localMember1.address()).start().join(),
        broadcastServiceFactory.newBroadcastService().start().join(),
        new GroupMembershipConfig());

    Member localMember2 = buildMember(2);
    ManagedClusterMembershipService clusterService2 = new DefaultClusterMembershipService(
        localMember2,
        bootstrapNodes,
        messagingServiceFactory.newMessagingService(localMember2.address()).start().join(),
        broadcastServiceFactory.newBroadcastService().start().join(),
        new GroupMembershipConfig());

    Member localMember3 = buildMember(3);
    ManagedClusterMembershipService clusterService3 = new DefaultClusterMembershipService(
        localMember3,
        bootstrapNodes,
        messagingServiceFactory.newMessagingService(localMember3.address()).start().join(),
        broadcastServiceFactory.newBroadcastService().start().join(),
        new GroupMembershipConfig());

    assertNull(clusterService1.getMember(MemberId.memberId("1")));
    assertNull(clusterService1.getMember(MemberId.memberId("2")));
    assertNull(clusterService1.getMember(MemberId.memberId("3")));

    CompletableFuture.allOf(new CompletableFuture[]{clusterService1.start(), clusterService2.start(),
        clusterService3.start()}).join();

    Thread.sleep(1000);

    assertEquals(3, clusterService1.getMembers().size());
    assertEquals(3, clusterService2.getMembers().size());
    assertEquals(3, clusterService3.getMembers().size());

    assertEquals(MemberId.Type.IDENTIFIED, clusterService1.getLocalMember().id().type());
    assertEquals(MemberId.Type.IDENTIFIED, clusterService1.getMember(MemberId.memberId("1")).id().type());
    assertEquals(MemberId.Type.IDENTIFIED, clusterService1.getMember(MemberId.memberId("2")).id().type());
    assertEquals(MemberId.Type.IDENTIFIED, clusterService1.getMember(MemberId.memberId("3")).id().type());

    assertEquals(State.ACTIVE, clusterService1.getLocalMember().getState());
    assertEquals(State.ACTIVE, clusterService1.getMember(MemberId.memberId("1")).getState());
    assertEquals(State.ACTIVE, clusterService1.getMember(MemberId.memberId("2")).getState());
    assertEquals(State.ACTIVE, clusterService1.getMember(MemberId.memberId("3")).getState());

    Member anonymousMember = buildMember(4);

    ManagedClusterMembershipService ephemeralClusterService = new DefaultClusterMembershipService(
        anonymousMember,
        bootstrapNodes,
        messagingServiceFactory.newMessagingService(anonymousMember.address()).start().join(),
        broadcastServiceFactory.newBroadcastService().start().join(),
        new GroupMembershipConfig());

    assertEquals(State.INACTIVE, ephemeralClusterService.getLocalMember().getState());

    assertNull(ephemeralClusterService.getMember(MemberId.memberId("1")));
    assertNull(ephemeralClusterService.getMember(MemberId.memberId("2")));
    assertNull(ephemeralClusterService.getMember(MemberId.memberId("3")));
    assertNull(ephemeralClusterService.getMember(MemberId.memberId("4")));
    assertNull(ephemeralClusterService.getMember(MemberId.memberId("5")));

    ephemeralClusterService.start().join();

    Thread.sleep(1000);

    assertEquals(4, clusterService1.getMembers().size());
    assertEquals(4, clusterService2.getMembers().size());
    assertEquals(4, clusterService3.getMembers().size());
    assertEquals(4, ephemeralClusterService.getMembers().size());

    clusterService1.stop().join();

    Thread.sleep(15000);

    assertEquals(3, clusterService2.getMembers().size());

    assertNull(clusterService2.getMember(MemberId.memberId("1")));
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.memberId("2")).getState());
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.memberId("3")).getState());
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.memberId("4")).getState());

    ephemeralClusterService.stop().join();

    Thread.sleep(15000);

    assertEquals(2, clusterService2.getMembers().size());
    assertNull(clusterService2.getMember(MemberId.memberId("1")));
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.memberId("2")).getState());
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.memberId("3")).getState());
    assertNull(clusterService2.getMember(MemberId.memberId("4")));

    Thread.sleep(2500);

    assertEquals(2, clusterService2.getMembers().size());

    assertNull(clusterService2.getMember(MemberId.memberId("1")));
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.memberId("2")).getState());
    assertEquals(State.ACTIVE, clusterService2.getMember(MemberId.memberId("3")).getState());
    assertNull(clusterService2.getMember(MemberId.memberId("4")));

    CompletableFuture.allOf(new CompletableFuture[]{clusterService1.stop(), clusterService2.stop(),
        clusterService3.stop()}).join();
  }
}
