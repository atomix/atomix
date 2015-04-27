/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.election;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.*;
import net.kuujo.copycat.protocol.raft.RaftProtocol;
import net.kuujo.copycat.protocol.raft.storage.BufferedStorage;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.UUID;
import java.util.function.Function;

/**
 * Leader election test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class LeaderElectionTest extends ConcurrentTestCase {
  private String testDirectory;

  @BeforeMethod
  public void setupDirectory() {
    testDirectory = String.format("test-logs/%s", UUID.randomUUID().toString());
  }

  @AfterMethod
  public void deleteDirectory() {
    if (testDirectory != null) {
      deleteDirectory(new File(testDirectory));
    }
  }

  /**
   * Tests a leader election.
   */
  public void testLeaderElection() throws Throwable {
    TestMemberRegistry registry = new TestMemberRegistry();

    TestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    TestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    TestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    LeaderElection election1 = buildElection(1, cluster1);
    LeaderElection election2 = buildElection(2, cluster2);
    LeaderElection election3 = buildElection(3, cluster3);

    EventListener<LeaderElectionEvent> listener = event -> {
      resume();
    };

    election1.addElectionListener(listener);
    election2.addElectionListener(listener);
    election3.addElectionListener(listener);

    expectResumes(4);

    election1.open().thenRun(this::resume);
    election2.open().thenRun(this::resume);
    election3.open().thenRun(this::resume);

    await();
  }

  /**
   * Tests a leader election after a network partition.
   */
  public void testLeaderElectionAfterPartition() throws Throwable {
    TestMemberRegistry registry = new TestMemberRegistry();

    TestCluster cluster1 = buildCluster(1, Member.Type.ACTIVE, 3, registry);
    TestCluster cluster2 = buildCluster(2, Member.Type.ACTIVE, 3, registry);
    TestCluster cluster3 = buildCluster(3, Member.Type.ACTIVE, 3, registry);

    LeaderElection election1 = buildElection(1, cluster1);
    LeaderElection election2 = buildElection(2, cluster2);
    LeaderElection election3 = buildElection(3, cluster3);

    Function<LeaderElection, EventListener<LeaderElectionEvent>> createListener = election -> {
      return new EventListener<LeaderElectionEvent>() {
        @Override
        public void accept(LeaderElectionEvent event) {
          election.removeElectionListener(this);

          resume();

          EventListener<LeaderElectionEvent> listener = internalEvent -> {
            resume();
          };

          election1.addElectionListener(listener);
          election2.addElectionListener(listener);
          election3.addElectionListener(listener);

          int id = event.cluster().member().id();
          cluster1.partition(id);
          cluster2.partition(id);
          cluster3.partition(id);
        }
      };
    };

    election1.addElectionListener(createListener.apply(election1));
    election2.addElectionListener(createListener.apply(election2));
    election3.addElectionListener(createListener.apply(election3));

    expectResumes(5);

    election1.open().thenRun(this::resume);
    election2.open().thenRun(this::resume);
    election3.open().thenRun(this::resume);

    await(10000);
  }

  /**
   * Creates a leader election.
   */
  private LeaderElection buildElection(int id, ManagedCluster cluster) throws Exception {
    return LeaderElection.builder()
      .withName("test")
      .withCluster(cluster)
      .withProtocol(buildProtocol(id, cluster))
      .build();
  }

  /**
   * Builds a Raft test cluster.
   */
  private TestCluster buildCluster(int id, Member.Type type, int nodes, TestMemberRegistry registry) {
    TestCluster.Builder builder = TestCluster.builder()
      .withRegistry(registry)
      .withLocalMember(TestLocalMember.builder()
        .withId(id)
        .withType(type)
        .withAddress(String.format("test-%d", id))
        .build());

    for (int i = 1; i <= nodes; i++) {
      if (i != id) {
        builder.addRemoteMember(TestRemoteMember.builder()
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
  private RaftProtocol buildProtocol(int id, ManagedCluster cluster) throws Exception {
    return (RaftProtocol) RaftProtocol.builder()
      .withStorage(BufferedStorage.builder()
        .withName(String.format("test-%d", id))
        .withDirectory(String.format("%s/test-%d", testDirectory, id))
        .build())
      .build();
  }

  /**
   * Deletes a directory after tests.
   */
  private static void deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if(file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
    }
    directory.delete();
  }

}
