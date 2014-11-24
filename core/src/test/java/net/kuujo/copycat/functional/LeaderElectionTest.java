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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.OperationEntry;
import net.kuujo.copycat.protocol.LocalProtocol;
import net.kuujo.copycat.spi.protocol.Protocol;
import net.kuujo.copycat.test.TestCluster;
import net.kuujo.copycat.test.TestLog;
import net.kuujo.copycat.test.TestNode;
import net.kuujo.copycat.test.TestStateMachine;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Leader election tests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class LeaderElectionTest {
  private Protocol protocol;
  private TestCluster cluster;

  @BeforeMethod
  protected void beforeMethod() {
    protocol = new LocalProtocol();
    cluster = new TestCluster();
  }

  @AfterMethod
  protected void afterMethod() {
    cluster.stop();
  }

  /**
   * Tests that a leader is elected in a single-node cluster.
   */
  public void testSingleNodeClusterLeaderIsElected() {
    TestNode node1 = new TestNode().withCluster("foo");
    cluster.addNode(node1);
    cluster.start();
    node1.await().electedLeader();
    assertTrue(node1.instance().isLeader());
  }

  /**
   * Tests that a leader is elected in a double-node cluster.
   */
  public void testTwoNodeClusterLeaderIsElected() {
    TestNode node1 = new TestNode().withCluster("foo", "bar").withProtocol(protocol);
    TestNode node2 = new TestNode().withCluster("bar", "foo").withProtocol(protocol);
    cluster.addNodes(node1, node2);
    cluster.start();
    node1.await().leaderElected();
    assertTrue(node1.instance().isLeader() || node2.instance().isLeader());
  }

  /**
   * Tests that a leader is elected in a triple-node cluster.
   */
  public void testThreeNodeClusterLeaderIsElected() {
    TestNode node1 = new TestNode().withCluster("foo", "bar", "baz").withProtocol(protocol);
    TestNode node2 = new TestNode().withCluster("bar", "foo", "baz").withProtocol(protocol);
    TestNode node3 = new TestNode().withCluster("baz", "foo", "bar").withProtocol(protocol);
    cluster.addNodes(node1, node2, node3);
    cluster.start();
    node1.await().leaderElected();
    assertTrue(node1.instance().isLeader() || node2.instance().isLeader()
      || node3.instance().isLeader());
  }

  /**
   * Tests that the candidate with the most up-to-date log is elected on startup.
   */
  public void testCandidateWithMostUpToDateLogIsElectedOnStartup() {
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode().withCluster("foo", "bar", "baz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("foo", "bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz")))
      .withState(CopycatState.CANDIDATE)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node2 = new TestNode().withCluster("bar", "foo", "baz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("bar", "foo", "baz")))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz")))
      .withState(CopycatState.CANDIDATE)
      .withCommitIndex(5)
      .withLastApplied(5);
    cluster.addNodes(node1, node2);
    cluster.start();

    node1.await().electedLeader();
    assertTrue(node1.instance().isLeader());
  }

  /**
   * Test candidate with most up-to-date log elected after failure.
   */
  public void testCandidateWithMostUpToDateLogIsElectedAfterFailure() {
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode().withCluster("foo", "bar", "baz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("foo", "bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz")))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(5)
      .withLastApplied(5)
      .withVotedFor(null);
    TestNode node2 = new TestNode().withCluster("bar", "foo", "baz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("bar", "foo", "baz")))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz")))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6)
      .withVotedFor(null);
    cluster.addNodes(node1, node2);
    cluster.start();

    node2.await().electedLeader();
    assertTrue(node2.instance().isLeader());
  }

  /**
   * Tests that candidates restart an election during a split vote.
   */
  public void testCandidatesIncrementTermAndRestartElectionDuringSplitVote() {
    TestNode node1 = new TestNode().withCluster("foo", "bar", "baz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("foo", "bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz")))
      .withState(CopycatState.CANDIDATE)
      .withCommitIndex(6)
      .withLastApplied(6)
      .withVotedFor("foo");
    TestNode node2 = new TestNode()
      .withCluster("bar", "foo", "baz").withProtocol(protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("bar", "foo", "baz")))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz")))
      .withState(CopycatState.CANDIDATE)
      .withCommitIndex(5)
      .withLastApplied(5)
      .withVotedFor("bar");
    cluster.addNodes(node1, node2);
    cluster.start();

    node1.await().electedLeader();
    assertTrue(node1.instance().isLeader());
    assertTrue(node1.instance().currentTerm() > 3);
  }

  /**
   * Tests that only a single leader is elected when more than one node is equal in terms of state.
   */
  public void testThatOneLeaderElectedWhenTwoNodesAreEqual() {
    TestNode node1 = new TestNode().withCluster("foo", "bar", "baz", "foobar", "barbaz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(
          new ConfigurationEntry(1, new Cluster("foo", "bar", "baz", "foobar", "barbaz")))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz")))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node2 = new TestNode().withCluster("bar", "foo", "baz", "foobar", "barbaz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(
          new ConfigurationEntry(1, new Cluster("bar", "foo", "baz", "foobar", "barbaz")))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz")))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node3 = new TestNode().withCluster("baz", "bar", "foo", "foobar", "barbaz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(
          new ConfigurationEntry(1, new Cluster("baz", "bar", "foo", "foobar", "barbaz")))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz")))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node4 = new TestNode().withCluster("foobar", "foo", "bar", "baz", "barbaz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader(null)
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(
          new ConfigurationEntry(1, new Cluster("foobar", "bar", "baz", "foo", "barbaz")))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(1, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz"))
          .withEntry(new OperationEntry(2, "foo", "bar", "baz")))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNodes(node1, node2, node3, node4);
    cluster.start();

    node1.await().leaderElected();

    int leaderCount = 0;
    for (TestNode node : cluster.nodes()) {
      if (node.instance().isLeader()) {
        leaderCount++;
      }
    }

    assertEquals(1, leaderCount);
  }
}
