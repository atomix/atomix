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
package net.kuujo.copycat.cluster;

import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

/**
 * Netty cluster test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class NettyClusterTest extends ConcurrentTestCase {

  /**
   * Builds a cluster.
   */
  private ManagedCluster buildCluster(int localMember, int members) {
    NettyCluster.Builder builder = NettyCluster.builder()
      .withLocalMember(NettyLocalMember.builder()
        .withId(localMember)
        .withType(Member.Type.ACTIVE)
        .withHost("localhost")
        .withPort(8080 + localMember)
        .build());

    for (int i = 0; i < members; i++) {
      if (i != localMember) {
        builder.addRemoteMember(NettyRemoteMember.builder()
          .withId(i)
          .withType(Member.Type.ACTIVE)
          .withHost("localhost")
          .withPort(8080 + i)
          .build());
      }
    }
    return builder.build();
  }

  /**
   * Tests connecting a remote member to a local member.
   */
  public void testConnectRemoteToLocal() throws Throwable {
    ManagedLocalMember localMember = NettyLocalMember.builder()
      .withHost("localhost")
      .withPort(8080)
      .build();
    expectResume();
    localMember.listen().thenRun(this::resume);
    await();

    ManagedRemoteMember remoteMember = NettyRemoteMember.builder()
      .withHost("localhost")
      .withPort(8080)
      .build();
    expectResume();
    remoteMember.connect().thenRun(this::resume);
    await();
  }

  /**
   * Tests connecting a remote member to a local member.
   */
  public void testConnectRemoteBeforeLocal() throws Throwable {
    ManagedRemoteMember remoteMember = NettyRemoteMember.builder()
      .withHost("localhost")
      .withPort(8080)
      .build();
    expectResumes(2);
    remoteMember.connect().thenRun(this::resume);

    ManagedLocalMember localMember = NettyLocalMember.builder()
      .withHost("localhost")
      .withPort(8080)
      .build();
    localMember.listen().thenRun(this::resume);
    await();
  }

  /**
   * Tests sending a message between remote and local members.
   */
  public void testMessageRemoteToLocal() throws Throwable {
    ManagedLocalMember localMember = NettyLocalMember.builder()
      .withHost("localhost")
      .withPort(8080)
      .build();
    expectResume();
    localMember.listen().thenRun(this::resume);
    await();

    ManagedRemoteMember remoteMember = NettyRemoteMember.builder()
      .withHost("localhost")
      .withPort(8080)
      .build();
    expectResume();
    remoteMember.connect().thenRun(this::resume);
    await();

    expectResume();
    localMember.registerHandler("test", message -> CompletableFuture.completedFuture("world!"));
    remoteMember.send("test", "Hello").whenComplete((result, error) -> {
      threadAssertNull(error);
      threadAssertEquals(result, "world!");
      resume();
    });
    await();
  }

  /**
   * Tests sending and receiving a message betwixt members.
   */
  public void testClusterSend() throws Throwable {
    ManagedCluster cluster1 = buildCluster(1, 3);
    ManagedCluster cluster2 = buildCluster(2, 3);
    ManagedCluster cluster3 = buildCluster(3, 3);

    expectResumes(3);

    cluster1.open().thenRun(this::resume);
    cluster2.open().thenRun(this::resume);
    cluster3.open().thenRun(this::resume);

    await();
  }

}
