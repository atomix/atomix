/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.protocol.raft;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.cluster.*;
import net.kuujo.copycat.protocol.raft.storage.BufferedStorage;
import net.kuujo.copycat.util.ExecutionContext;
import org.testng.annotations.Test;

/**
 * Raft protocol test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class RaftProtocolTest extends ConcurrentTestCase {

  /**
   * Tests that the protocol elects a leader.
   */
  public void testElectLeader() throws Throwable {
    RaftTestMemberRegistry registry = new RaftTestMemberRegistry();

    RaftProtocol protocol1 = buildProtocol(1, 3, registry);
    RaftProtocol protocol2 = buildProtocol(2, 3, registry);
    RaftProtocol protocol3 = buildProtocol(3, 3, registry);

    expectResumes(3);

    protocol1.open().thenRun(this::resume);
    protocol2.open().thenRun(this::resume);
    protocol3.open().thenRun(this::resume);

    await();
  }

  /**
   * Builds a Raft test cluster.
   */
  private RaftTestCluster buildCluster(int id, int nodes, RaftTestMemberRegistry registry) {
    RaftTestCluster.Builder builder = RaftTestCluster.builder()
      .withRegistry(registry)
      .withLocalMember(RaftTestLocalMember.builder()
        .withId(id)
        .withType(Member.Type.ACTIVE)
        .withAddress(String.format("test-%d", id))
        .build());

    for (int i = 1; i <= nodes; i++) {
      if (i != id) {
        builder.addRemoteMember(RaftTestRemoteMember.builder()
          .withId(i)
          .withType(Member.Type.ACTIVE)
          .withAddress(String.format("test-%d", i))
          .build());
      }
    }

    return builder.build();
  }

  /**
   * Creates a Raft protocol for the given node.
   */
  private RaftProtocol buildProtocol(int id, int nodes, RaftTestMemberRegistry registry) {
    ManagedCluster cluster = buildCluster(id, nodes, registry);
    cluster.open().join();

    RaftProtocol protocol = (RaftProtocol) RaftProtocol.builder()
      .withContext(new ExecutionContext("test-" + id))
      .withStorage(BufferedStorage.builder()
        .withName(String.format("test-%d", id))
        .withDirectory(String.format("test-logs/test-%d", id))
        .build())
      .build();

    protocol.setCluster(cluster);
    protocol.setTopic("test");
    return protocol;
  }

}
