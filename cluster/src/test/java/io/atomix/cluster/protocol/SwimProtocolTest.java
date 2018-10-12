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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
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
import io.atomix.utils.Version;
import io.atomix.utils.net.Address;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.atomix.cluster.protocol.GroupMembershipEvent.Type.MEMBER_ADDED;
import static io.atomix.cluster.protocol.GroupMembershipEvent.Type.MEMBER_REMOVED;
import static io.atomix.cluster.protocol.GroupMembershipEvent.Type.METADATA_CHANGED;
import static io.atomix.cluster.protocol.GroupMembershipEvent.Type.REACHABILITY_CHANGED;
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

  private Version version1 = Version.from("1.0.0");
  private Version version2 = Version.from("2.0.0");

  private Map<MemberId, SwimMembershipProtocol> protocols = Maps.newConcurrentMap();
  private Map<MemberId, TestGroupMembershipEventListener> listeners = Maps.newConcurrentMap();

  private Member member(String id, String host, int port, Version version) {
    return new SwimMembershipProtocol.SwimMember(
        MemberId.from(id),
        new Address(host, port),
        null,
        null,
        null,
        new Properties(),
        version,
        System.currentTimeMillis());
  }

  @Before
  @SuppressWarnings("unchecked")
  public void reset() {
    messagingServiceFactory = new TestMessagingServiceFactory();
    unicastServiceFactory = new TestUnicastServiceFactory();
    broadcastServiceFactory = new TestBroadcastServiceFactory();

    member1 = member("1", "localhost", 5001, version1);
    member2 = member("2", "localhost", 5002, version1);
    member3 = member("3", "localhost", 5003, version1);
    members = Arrays.asList(member1, member2, member3);
    nodes = (Collection) members;
    listeners = Maps.newConcurrentMap();
  }

  @Test
  public void testSwimProtocol() throws Exception {
    // Start a node and check its events.
    startProtocol(member1);
    checkEvent(member1, MEMBER_ADDED, member1);
    checkMembers(member1, member1);

    // Start a node and check its events.
    startProtocol(member2);
    checkEvent(member2, MEMBER_ADDED, member2);
    checkEvent(member2, MEMBER_ADDED, member1);
    checkMembers(member2, member1, member2);
    checkEvent(member1, MEMBER_ADDED, member2);
    checkMembers(member1, member1, member2);

    // Start a node and check its events.
    startProtocol(member3);
    checkEvent(member3, MEMBER_ADDED, member3);
    checkEvent(member3, MEMBER_ADDED);
    checkEvent(member3, MEMBER_ADDED);
    checkMembers(member3, member1, member2, member3);
    checkEvent(member2, MEMBER_ADDED, member3);
    checkMembers(member2, member1, member2, member3);
    checkEvent(member1, MEMBER_ADDED, member3);
    checkMembers(member1, member1, member2, member3);

    // Isolate node 3 from the rest of the cluster.
    partition(member3);

    // Nodes 1 and 2 should see REACHABILITY_CHANGED events and then MEMBER_REMOVED events.
    checkEvent(member1, REACHABILITY_CHANGED, member3);
    checkEvent(member2, REACHABILITY_CHANGED, member3);
    checkEvent(member1, MEMBER_REMOVED, member3);
    checkEvent(member2, MEMBER_REMOVED, member3);

    // Verify that node 3 was removed from nodes 1 and 2.
    checkMembers(member1, member1, member2);
    checkMembers(member2, member1, member2);

    // Node 3 should also see REACHABILITY_CHANGED and MEMBER_REMOVED events for nodes 1 and 2.
    checkEvents(
        member3,
        new GroupMembershipEvent(REACHABILITY_CHANGED, member1),
        new GroupMembershipEvent(REACHABILITY_CHANGED, member2),
        new GroupMembershipEvent(MEMBER_REMOVED, member1),
        new GroupMembershipEvent(MEMBER_REMOVED, member2));

    // Verify that nodes 1 and 2 were removed from node 3.
    checkMembers(member3, member3);

    // Heal the partition.
    heal(member3);

    // Verify that the nodes discovery each other again.
    checkEvent(member1, MEMBER_ADDED, member3);
    checkEvent(member2, MEMBER_ADDED, member3);
    checkEvents(
        member3,
        new GroupMembershipEvent(MEMBER_ADDED, member1),
        new GroupMembershipEvent(MEMBER_ADDED, member2));

    // Partition node 1 from node 2.
    partition(member1, member2);

    // Verify that neither node is ever removed from the cluster since node 3 can still ping nodes 1 and 2.
    Thread.sleep(5000);
    checkMembers(member1, member1, member2, member3);
    checkMembers(member2, member1, member2, member3);
    checkMembers(member3, member1, member2, member3);

    // Heal the partition.
    heal(member1, member2);

    // Update node 1's metadata.
    member1.properties().put("foo", "bar");

    // Verify the metadata change is propagated throughout the cluster.
    checkEvent(member1, METADATA_CHANGED, member1);
    checkEvent(member2, METADATA_CHANGED, member1);
    checkEvent(member3, METADATA_CHANGED, member1);

    // Stop member 3 and change its version.
    stopProtocol(member3);
    Member member = member(member3.id().id(), member3.address().host(), member3.address().port(), version2);
    startProtocol(member);

    // Verify that version 1 is removed and version 2 is added.
    checkEvent(member1, MEMBER_REMOVED, member3);
    checkEvent(member1, MEMBER_ADDED, member);
    checkEvent(member2, MEMBER_REMOVED, member3);
    checkEvent(member2, MEMBER_ADDED, member);
  }

  private SwimMembershipProtocol startProtocol(Member member) {
    SwimMembershipProtocol protocol = new SwimMembershipProtocol(new SwimMembershipProtocolConfig()
        .setFailureTimeout(Duration.ofSeconds(2)));
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
    protocols.put(member.id(), protocol);
    return protocol;
  }

  private void stopProtocol(Member member) {
    SwimMembershipProtocol protocol = protocols.remove(member.id());
    if (protocol != null) {
      protocol.leave(member).join();
    }
  }

  private void partition(Member member) {
    unicastServiceFactory.partition(member.address());
    messagingServiceFactory.partition(member.address());
  }

  private void partition(Member member1, Member member2) {
    unicastServiceFactory.partition(member1.address(), member2.address());
    messagingServiceFactory.partition(member1.address(), member2.address());
  }

  private void heal(Member member) {
    unicastServiceFactory.heal(member.address());
    messagingServiceFactory.heal(member.address());
  }

  private void heal(Member member1, Member member2) {
    unicastServiceFactory.heal(member1.address(), member2.address());
    messagingServiceFactory.heal(member1.address(), member2.address());
  }

  private void checkMembers(Member member, Member... members) {
    SwimMembershipProtocol protocol = protocols.get(member.id());
    assertEquals(Sets.newHashSet(members), protocol.getMembers());
  }

  private void checkEvents(Member member, GroupMembershipEvent... types) throws InterruptedException {
    Multiset<GroupMembershipEvent> events = HashMultiset.create(Arrays.asList(types));
    for (int i = 0; i < types.length; i++) {
      GroupMembershipEvent event = nextEvent(member);
      if (!events.remove(event)) {
        throw new AssertionError();
      }
    }
  }

  private void checkEvent(Member member, GroupMembershipEvent.Type type) throws InterruptedException {
    checkEvent(member, type, null);
  }

  private void checkEvent(Member member, GroupMembershipEvent.Type type, Member value) throws InterruptedException {
    GroupMembershipEvent event = nextEvent(member);
    assertEquals(type, event.type());
    if (value != null) {
      assertEquals(value, event.member());
    }
  }

  private GroupMembershipEvent nextEvent(Member member) throws InterruptedException {
    TestGroupMembershipEventListener listener = listeners.get(member.id());
    return listener != null ? listener.nextEvent() : null;
  }

  private class TestGroupMembershipEventListener implements GroupMembershipEventListener {
    private BlockingQueue<GroupMembershipEvent> queue = new ArrayBlockingQueue<>(10);

    @Override
    public void event(GroupMembershipEvent event) {
      queue.add(event);
    }

    GroupMembershipEvent nextEvent() throws InterruptedException {
      return queue.poll(10, TimeUnit.SECONDS);
    }
  }
}
