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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;

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
 * Configuration replication tests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ConfigurationTest {
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
   * Tests that the leader's expanded configuration is logged and replicated.
   */
  public void testLeaderReplicatesExpandedConfiguration() {
    TestNode node1 = new TestNode().withCluster("foo", "bar", "baz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("foo", "bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node2 = new TestNode().withCluster("bar", "foo", "baz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("bar", "foo", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node3 = new TestNode().withCluster("baz", "bar", "foo")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(new ConfigurationEntry(1, new Cluster("baz", "foo", "bar")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.LEADER)
      .withCommitIndex(6)
      .withLastApplied(6);

    cluster.addNodes(node1, node2, node3);
    cluster.start();

    node3.instance().cluster().addRemoteMember("foobarbaz");

    // First, the leader should have replicated a join configuration.
    node1.await().membershipChange();
    assertTrue(node1.instance().cluster().containsMember("foobarbaz"));
  }

  /**
   * Tests that the leader's reduced cluster configuration is logged and replicated.
   */
  public void testLeaderReplicatesReducedConfiguration() {
    TestNode node1 = new TestNode().withCluster("foo", "bar", "baz", "foobarbaz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(
          new ConfigurationEntry(1, new Cluster("foo", "bar", "baz", "foobarbaz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node2 = new TestNode().withCluster("bar", "foo", "baz", "foobarbaz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(
          new ConfigurationEntry(1, new Cluster("bar", "foo", "baz", "foobarbaz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.FOLLOWER)
      .withCommitIndex(6)
      .withLastApplied(6);
    TestNode node3 = new TestNode().withCluster("baz", "bar", "foo", "foobarbaz")
      .withProtocol(protocol)
      .withTerm(3)
      .withLeader("baz")
      .withStateMachine(new TestStateMachine())
      .withLog(
        new TestLog().withEntry(
          new ConfigurationEntry(1, new Cluster("baz", "foo", "bar", "foobarbaz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz")))
          .withEntry(new OperationEntry(2, "foo", Arrays.asList("bar", "baz"))))
      .withState(CopycatState.LEADER)
      .withCommitIndex(6)
      .withLastApplied(6);

    cluster.addNodes(node1, node2, node3);
    cluster.start();

    node3.instance().cluster().removeRemoteMember("foobarbaz");

    // First, the leader should have replicated a join configuration.
    if (node1.instance().cluster().remoteMember("foobarbaz") == null) {
      assertFalse(node1.instance().cluster().containsMember("foobarbaz"));
    } else {
      node1.await().membershipChange();
      assertFalse(node1.instance().cluster().containsMember("foobarbaz"));
    }
  }
}
