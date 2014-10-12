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
   * Tests that the candidate with the most up-to-date log is elected.
   */
  public void testCandidateWithMostUpToDateLogIsElected() {
    TestCluster cluster = new TestCluster();
    TestNode node1 = new TestNode("foo")
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

    TestNode node2 = new TestNode("bar")
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
      .withCommitIndex(6)
      .withLastApplied(6);
    cluster.addNode(node2);

    cluster.start();

    node1.await().electedLeader();
    Assert.assertTrue(node1.instance().isLeader());
  }

}
