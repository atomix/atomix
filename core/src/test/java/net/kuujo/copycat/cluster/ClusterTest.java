/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.internal.cluster.ClusterManager;
import net.kuujo.copycat.protocol.AsyncLocalProtocol;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.testng.Assert.*;

/**
 * Cluster test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ClusterTest {
  public void testMemberConfigure() {
    MemberConfig config = new MemberConfig();
    config.setId("foo");
    assertEquals("foo", config.getId());
    config.withId("bar");
    assertEquals("bar", config.getId());
    Member member = new Member(config);
    assertEquals("bar", member.id());
  }

  public void testClusterConfigure() {
    ClusterConfig<Member> config = new ClusterConfig<>();
    Member localMember = new Member(new MemberConfig("foo"));
    config.setLocalMember(localMember);
    assertEquals(localMember, config.getLocalMember());
    Set<Member> remoteMembers = new HashSet<>(2);
    Member remoteMember1 = new Member(new MemberConfig("bar"));
    Member remoteMember2 = new Member(new MemberConfig("baz"));
    remoteMembers.add(remoteMember1);
    remoteMembers.add(remoteMember2);
    config.setRemoteMembers(remoteMembers);
    assertTrue(config.getRemoteMembers().contains(remoteMember1));
    assertTrue(config.getRemoteMembers().contains(remoteMember2));
    Cluster<Member> cluster = new LocalCluster(config);
    assertEquals(localMember, cluster.localMember());
    assertTrue(cluster.remoteMembers().contains(remoteMember1));
    assertTrue(cluster.remoteMembers().contains(remoteMember2));
    assertEquals(remoteMember1, cluster.remoteMember("bar"));
    assertEquals(remoteMember2, cluster.remoteMember("baz"));
    assertEquals(localMember, cluster.member("foo"));
    assertEquals(remoteMember1, cluster.member("bar"));
    assertEquals(remoteMember2, cluster.member("baz"));
  }

  public void testClusterReconfigure() {
    ClusterConfig<Member> config = new ClusterConfig<>();
    Member localMember = new Member(new MemberConfig("foo"));
    config.setLocalMember(localMember);
    Set<Member> remoteMembers = new HashSet<>(2);
    Member remoteMember1 = new Member(new MemberConfig("bar"));
    Member remoteMember2 = new Member(new MemberConfig("baz"));
    remoteMembers.add(remoteMember1);
    remoteMembers.add(remoteMember2);
    config.setRemoteMembers(remoteMembers);
    Cluster<Member> cluster = new LocalCluster(config);
    assertEquals(localMember, cluster.localMember());
    assertEquals(remoteMember1, cluster.remoteMember("bar"));
    assertEquals(remoteMember2, cluster.remoteMember("baz"));
    assertEquals(2, cluster.remoteMembers().size());
    Member remoteMember3 = new Member(new MemberConfig("foobarbaz"));
    config.addRemoteMember(remoteMember3);
    assertEquals(remoteMember3, cluster.remoteMember("foobarbaz"));
  }

  public void testClusterCopy() {
    ClusterConfig<Member> config = new ClusterConfig<>();
    Member localMember = new Member(new MemberConfig("foo"));
    config.setLocalMember(localMember);
    Set<Member> remoteMembers = new HashSet<>(2);
    Member remoteMember1 = new Member(new MemberConfig("bar"));
    Member remoteMember2 = new Member(new MemberConfig("baz"));
    remoteMembers.add(remoteMember1);
    remoteMembers.add(remoteMember2);
    config.setRemoteMembers(remoteMembers);
    Cluster<Member> cluster = new LocalCluster(config);
    Cluster<Member> copy = cluster.copy();
    assertNotSame(copy, cluster);
    assertEquals(cluster.localMember(), copy.localMember());
    assertEquals(cluster.remoteMembers(), copy.remoteMembers());
  }

  public void testClusterCopyConfiguration() {
    ClusterConfig<Member> config = new ClusterConfig<>();
    Member localMember = new Member(new MemberConfig("foo"));
    config.setLocalMember(localMember);
    Set<Member> remoteMembers = new HashSet<>(2);
    Member remoteMember1 = new Member(new MemberConfig("bar"));
    Member remoteMember2 = new Member(new MemberConfig("baz"));
    remoteMembers.add(remoteMember1);
    remoteMembers.add(remoteMember2);
    config.setRemoteMembers(remoteMembers);
    ClusterConfig<Member> copy = config.copy();
    assertNotSame(copy, config);
    assertEquals(config.getLocalMember(), copy.getLocalMember());
    assertEquals(config.getRemoteMembers(), copy.getRemoteMembers());
  }

  public void testClusterManager() {
    ClusterConfig<Member> config = new ClusterConfig<>();
    Member localMember = new Member(new MemberConfig("foo"));
    config.setLocalMember(localMember);
    Set<Member> remoteMembers = new HashSet<>(2);
    Member remoteMember1 = new Member(new MemberConfig("bar"));
    Member remoteMember2 = new Member(new MemberConfig("baz"));
    remoteMembers.add(remoteMember1);
    remoteMembers.add(remoteMember2);
    config.setRemoteMembers(remoteMembers);
    Cluster<Member> cluster = new LocalCluster(config);
    ClusterManager<Member> clusterManager = new ClusterManager<>(cluster, new AsyncLocalProtocol());
    assertEquals(localMember, clusterManager.localNode().member());
    assertEquals(2, clusterManager.remoteNodes().size());
    assertEquals(remoteMember1, clusterManager.remoteNode("bar").member());
    assertEquals(remoteMember2, clusterManager.remoteNode("baz").member());
  }

  public void testClusterManagerNotExternallyConfigurable() {
    ClusterConfig<Member> config = new ClusterConfig<>();
    Member localMember = new Member(new MemberConfig("foo"));
    config.setLocalMember(localMember);
    Set<Member> remoteMembers = new HashSet<>(2);
    Member remoteMember1 = new Member(new MemberConfig("bar"));
    Member remoteMember2 = new Member(new MemberConfig("baz"));
    remoteMembers.add(remoteMember1);
    remoteMembers.add(remoteMember2);
    config.setRemoteMembers(remoteMembers);
    Cluster<Member> cluster = new LocalCluster(config);
    ClusterManager<Member> clusterManager = new ClusterManager<>(cluster, new AsyncLocalProtocol());
    assertEquals(localMember, clusterManager.localNode().member());
    assertEquals(2, clusterManager.remoteNodes().size());
    assertEquals(remoteMember1, clusterManager.remoteNode("bar").member());
    assertEquals(remoteMember2, clusterManager.remoteNode("baz").member());
    Member remoteMember3 = new Member(new MemberConfig("foobarbaz"));
    config.addRemoteMember(remoteMember3);
    assertEquals(remoteMember3, cluster.remoteMember("foobarbaz"));
    assertNull(clusterManager.remoteNode("foobarbaz"));
  }

  public void testClusterManagerInternalReconfigure() {
    ClusterConfig<Member> config = new ClusterConfig<>();
    Member localMember = new Member(new MemberConfig("foo"));
    config.setLocalMember(localMember);
    Set<Member> remoteMembers = new HashSet<>(2);
    Member remoteMember1 = new Member(new MemberConfig("bar"));
    Member remoteMember2 = new Member(new MemberConfig("baz"));
    remoteMembers.add(remoteMember1);
    remoteMembers.add(remoteMember2);
    config.setRemoteMembers(remoteMembers);
    Cluster<Member> cluster = new LocalCluster(config);
    ClusterManager<Member> clusterManager = new ClusterManager<>(cluster, new AsyncLocalProtocol());
    assertEquals(localMember, clusterManager.localNode().member());
    assertEquals(2, clusterManager.remoteNodes().size());
    assertEquals(remoteMember1, clusterManager.remoteNode("bar").member());
    assertEquals(remoteMember2, clusterManager.remoteNode("baz").member());
    Member remoteMember3 = new Member(new MemberConfig("foobarbaz"));
    clusterManager.cluster().config().addRemoteMember(remoteMember3);
    assertEquals(remoteMember3, clusterManager.cluster().remoteMember("foobarbaz"));
    assertEquals(remoteMember3, clusterManager.remoteNode("foobarbaz").member());
  }

}
