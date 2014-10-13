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
 * Leader election tests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class LeaderElectionTest {

  /**
   * Tests that a leader is elected in a single-node cluster.
   */
  public void testSingleNodeClusterLeaderIsElected() {
    AsyncProtocol<Member> protocol = new AsyncLocalProtocol();
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode(new Member("foo"), protocol);
    cluster.addNode(node1);
    cluster.start();
    node1.await().electedLeader();
    Assert.assertTrue(node1.instance().isLeader());
  }

  /**
   * Tests that a leader is elected in a double-node cluster.
   */
  public void testTwoNodeClusterLeaderIsElected() {
    AsyncProtocol<Member> protocol = new AsyncLocalProtocol();
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode(new Member("foo"), protocol);
    cluster.addNode(node1);
    TestNode node2 = new TestNode(new Member("bar"), protocol);
    cluster.addNode(node2);
    cluster.start();
    node1.await().leaderElected();
    Assert.assertTrue(node1.instance().isLeader() || node2.instance().isLeader());
  }

  /**
   * Tests that a leader is elected in a triple-node cluster.
   */
  public void testThreeNodeClusterLeaderIsElected() {
    AsyncProtocol<Member> protocol = new AsyncLocalProtocol();
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode(new Member("foo"), protocol);
    cluster.addNode(node1);
    TestNode node2 = new TestNode(new Member("bar"), protocol);
    cluster.addNode(node2);
    TestNode node3 = new TestNode(new Member("baz"), protocol);
    cluster.addNode(node3);
    cluster.start();
    node1.await().leaderElected();
    Assert.assertTrue(node1.instance().isLeader() || node2.instance().isLeader() || node3.instance().isLeader());
  }

  /**
   * Tests that the candidate with the most up-to-date log is elected on startup.
   */
  public void testCandidateWithMostUpToDateLogIsElectedOnStartup() {
    AsyncProtocol<Member> protocol = new AsyncLocalProtocol();
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode(new Member("foo"), protocol)
      .withTerm(3)
      .withLeader(null)
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
      .withState(CopycatState.CANDIDATE)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node1);

    TestNode node2 = new TestNode(new Member("bar"), protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("bar"))
          .withRemoteMembers(new Member("foo"), new Member("baz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.CANDIDATE)
      .withCommitIndex(5)
      .withLastApplied(5);
    cluster.addNode(node2);

    cluster.start();

    node1.await().electedLeader();
    Assert.assertTrue(node1.instance().isLeader());
  }

  /**
   * Test candidate with most up-to-date log elected after failure.
   */
  public void testCandidateWithMostUpToDateLogIsElectedAfterFailure() {
    AsyncProtocol<Member> protocol = new AsyncLocalProtocol();
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode(new Member("foo"), protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("foo"))
          .withRemoteMembers(new Member("bar"), new Member("baz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(5)
      .withLastApplied(5);
    cluster.addNode(node1);

    TestNode node2 = new TestNode(new Member("bar"), protocol)
      .withTerm(3)
      .withLeader(null)
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

    cluster.start();

    node2.await().electedLeader();
    Assert.assertTrue(node2.instance().isLeader());
  }

  /**
   * Tests that candidates restart an election during a split vote.
   */
  public void testCandidatesIncrementTermAndRestartElectionDuringSplitVote() {
    AsyncProtocol<Member> protocol = new AsyncLocalProtocol();
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode(new Member("foo"), protocol)
      .withTerm(3)
      .withLeader(null)
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
      .withState(CopycatState.CANDIDATE)
      .withCommitIndex(6)
      .withLastApplied(6)
      .withVotedFor("foo");
    cluster.addNode(node1);

    TestNode node2 = new TestNode(new Member("bar"), protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("bar"))
          .withRemoteMembers(new Member("foo"), new Member("baz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.CANDIDATE)
      .withCommitIndex(5)
      .withLastApplied(5)
      .withVotedFor("bar");
    cluster.addNode(node2);

    cluster.start();

    node1.await().electedLeader();
    Assert.assertTrue(node1.instance().isLeader());
    Assert.assertTrue(node1.instance().currentTerm() > 3);
  }

  /**
   * Tests that only a single leader is elected when more than one node is equal in terms of state.
   */
  public void testThatOneLeaderElectedWhenTwoNodesAreEqual() {
    AsyncProtocol<Member> protocol = new AsyncLocalProtocol();
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode(new Member("foo"), protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("foo"))
          .withRemoteMembers(new Member("bar"), new Member("baz"), new Member("foobar"), new Member("barbaz"))))
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
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("bar"))
          .withRemoteMembers(new Member("foo"), new Member("baz"), new Member("foobar"), new Member("barbaz"))))
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
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("baz"))
          .withRemoteMembers(new Member("bar"), new Member("foo"), new Member("foobar"), new Member("barbaz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node3);

    TestNode node4 = new TestNode(new Member("foobar"), protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(new TestLog()
        .withEntry(new ConfigurationEntry(1, new ClusterConfig()
          .withLocalMember(new Member("foobar"))
          .withRemoteMembers(new Member("bar"), new Member("baz"), new Member("foo"), new Member("barbaz"))))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
        .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node4);

    cluster.start();

    node1.await().leaderElected();

    int leaderCount = 0;
    for (TestNode node : cluster.nodes()) {
      if (node.instance().isLeader()) {
        leaderCount++;
      }
    }

    Assert.assertEquals(1, leaderCount);
  }

}
