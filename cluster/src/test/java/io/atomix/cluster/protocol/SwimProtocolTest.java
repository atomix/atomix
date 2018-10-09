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
package io.atomix.cluster.protocol;

import com.google.common.collect.Maps;
import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.Node;
import io.atomix.cluster.TestBootstrapService;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryService;
import io.atomix.cluster.impl.DefaultNodeDiscoveryService;
import io.atomix.cluster.messaging.impl.TestBroadcastServiceFactory;
import io.atomix.cluster.messaging.impl.TestMessagingServiceFactory;
import io.atomix.cluster.messaging.impl.TestUnicastServiceFactory;
import io.atomix.utils.net.Address;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * SWIM membership protocol test.
 */
public class SwimProtocolTest extends ConcurrentTestCase {
  private TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();
  private TestUnicastServiceFactory unicastServiceFactory = new TestUnicastServiceFactory();
  private TestBroadcastServiceFactory broadcastServiceFactory = new TestBroadcastServiceFactory();

  private Member member1;
  private Member member2;
  private Member member3;
  private Collection<Member> members;
  private Collection<Node> nodes;

  private Map<MemberId, TestGroupMembershipEventListener> listeners = Maps.newConcurrentMap();

  @Before
  @SuppressWarnings("unchecked")
  public void reset() {
    messagingServiceFactory = new TestMessagingServiceFactory();
    unicastServiceFactory = new TestUnicastServiceFactory();
    broadcastServiceFactory = new TestBroadcastServiceFactory();

    member1 = Member.builder()
        .withId("1")
        .withAddress(new Address("localhost", 5001))
        .build();
    member2 = Member.builder()
        .withId("2")
        .withAddress(new Address("localhost", 5002))
        .build();
    member3 = Member.builder()
        .withId("3")
        .withAddress(new Address("localhost", 5003))
        .build();
    members = Arrays.asList(member1, member2, member3);
    nodes = (Collection) members;
    listeners = Maps.newConcurrentMap();
  }

  @Test
  public void testSwimProtocol() throws Exception {
    SwimMembershipProtocol protocol1 = startProtocol(member1);
    GroupMembershipEvent event1;

    event1 = nextEvent(member1);
    assertEquals(GroupMembershipEvent.Type.MEMBER_ADDED, event1.type());
    assertEquals(member1, event1.member());

    SwimMembershipProtocol protocol2 = startProtocol(member2);
    GroupMembershipEvent event2;

    event2 = nextEvent(member2);
    assertEquals(GroupMembershipEvent.Type.MEMBER_ADDED, event2.type());
    assertEquals(member2, event2.member());

    event1 = nextEvent(member1);
    assertEquals(GroupMembershipEvent.Type.MEMBER_ADDED, event1.type());
    assertEquals(member2, event1.member());
  }

  private SwimMembershipProtocol startProtocol(Member member) {
    SwimMembershipProtocol protocol = new SwimMembershipProtocol(new SwimMembershipProtocolConfig());
    TestGroupMembershipEventListener listener = new TestGroupMembershipEventListener();
    listeners.put(member.id(), listener);
    protocol.addListener(listener);
    BootstrapService bootstrap = new TestBootstrapService(
        messagingServiceFactory.newMessagingService(member.address()).start().join(),
        unicastServiceFactory.newUnicastService(member.address()).start().join(),
        broadcastServiceFactory.newBroadcastService().start().join());
    NodeDiscoveryProvider provider = new BootstrapDiscoveryProvider(nodes);
    provider.join(bootstrap, member).join();
    NodeDiscoveryService discovery = new DefaultNodeDiscoveryService(bootstrap, member, provider).start().join();
    protocol.join(bootstrap, discovery, member).join();
    return protocol;
  }

  private GroupMembershipEvent nextEvent(Member member) {
    TestGroupMembershipEventListener listener = listeners.get(member.id());
    return listener != null ? listener.nextEvent() : null;
  }

  private class TestGroupMembershipEventListener implements GroupMembershipEventListener {
    private BlockingQueue<GroupMembershipEvent> queue = new ArrayBlockingQueue<>(10);

    @Override
    public void event(GroupMembershipEvent event) {
      queue.add(event);
    }

    GroupMembershipEvent nextEvent() {
      try {
        return queue.poll(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        return null;
      }
    }
  }
}
