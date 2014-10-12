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
package net.kuujo.copycat.functional;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.OperationEntry;
import net.kuujo.copycat.protocol.AsyncLocalProtocol;
import net.kuujo.copycat.spi.protocol.AsyncProtocol;
import net.kuujo.copycat.test.TestCluster;
import net.kuujo.copycat.test.TestLog;
import net.kuujo.copycat.test.TestNode;
import net.kuujo.copycat.test.TestStateMachine;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Configuration replication tests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class ConfigurationTest {

  /**
   * Tests that the leader's expanded configuration is logged and replicated.
   */
  public void testLeaderReplicatesExpandedConfiguration() {
    AsyncProtocol<Member> protocol = new AsyncLocalProtocol();
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode(new Member("foo"), protocol)
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("foo"))
          .withRemoteMembers(new Member("bar"), new Member("baz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node1);

    TestNode node2 = new TestNode(new Member("bar"), protocol)
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("bar"))
          .withRemoteMembers(new Member("foo"), new Member("baz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node2);

    TestNode node3 = new TestNode(new Member("baz"), protocol)
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("baz"))
          .withRemoteMembers(new Member("foo"), new Member("bar"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.LEADER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node3);

    cluster.start();

    node3.instance().cluster().config().addRemoteMember(new Member("foobarbaz"));

    // First, the leader should have replicated a join configuration.
    node1.await().membershipChange();
    Assert.assertNotNull(node1.instance().clusterManager().cluster().remoteMember("foobarbaz"));

    // Next, the leader should have replicated the new configuration.
    node1.await().membershipChange();
    Assert.assertNotNull(node1.instance().clusterManager().cluster().remoteMember("foobarbaz"));
  }

  /**
   * Tests that the leader's reduced cluster configuration is logged and replicated.
   */
  public void testLeaderReplicatesReducedConfiguration() {

  }

}
