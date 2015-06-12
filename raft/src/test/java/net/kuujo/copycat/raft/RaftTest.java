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
package net.kuujo.copycat.raft;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.cluster.NettyCluster;
import net.kuujo.copycat.cluster.NettyMember;
import net.kuujo.copycat.raft.log.Log;
import net.kuujo.copycat.raft.log.StorageLevel;
import org.testng.annotations.Test;

/**
 * Raft test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class RaftTest extends ConcurrentTestCase {

  /**
   * Tests putting and getting a value with a Netty cluster.
   */
  public void testNetty() throws Throwable {
    Raft raft1 = Raft.builder()
      .withCluster(NettyCluster.builder()
        .withMemberId(1)
        .withHost("localhost")
        .withPort(5001)
        .addMember(NettyMember.builder()
          .withId(2)
          .withHost("localhost")
          .withPort(5002)
          .build())
        .addMember(NettyMember.builder()
          .withId(3)
          .withHost("localhost")
          .withPort(5003)
          .build())
        .build())
      .withStateMachine(new TestStateMachine())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();

    Raft raft2 = Raft.builder()
      .withCluster(NettyCluster.builder()
        .withMemberId(2)
        .withHost("localhost")
        .withPort(5002)
        .addMember(NettyMember.builder()
          .withId(1)
          .withHost("localhost")
          .withPort(5001)
          .build())
        .addMember(NettyMember.builder()
          .withId(3)
          .withHost("localhost")
          .withPort(5003)
          .build())
        .build())
      .withStateMachine(new TestStateMachine())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();

    Raft raft3 = Raft.builder()
      .withCluster(NettyCluster.builder()
        .withMemberId(3)
        .withHost("localhost")
        .withPort(5003)
        .addMember(NettyMember.builder()
          .withId(1)
          .withHost("localhost")
          .withPort(5001)
          .build())
        .addMember(NettyMember.builder()
          .withId(2)
          .withHost("localhost")
          .withPort(5002)
          .build())
        .build())
      .withStateMachine(new TestStateMachine())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();

    expectResumes(3);
    raft1.open().thenRun(this::resume);
    raft2.open().thenRun(this::resume);
    raft3.open().thenRun(this::resume);
    await();

    Thread.sleep(1000);

    if (raft1.state() == Raft.State.LEADER) {
      raft1.close().join();
    }
    if (raft2.state() == Raft.State.LEADER) {
      raft2.close().join();
    }
    if (raft3.state() == Raft.State.LEADER) {
      raft3.close().join();
    }

    while (true);
  }

  /**
   * Tests putting and getting a value with a Netty cluster.
   */
  public void testNettyClient() throws Throwable {
    Raft raft1 = Raft.builder()
      .withCluster(NettyCluster.builder()
        .withMemberId(1)
        .withHost("localhost")
        .withPort(5001)
        .addMember(NettyMember.builder()
          .withId(2)
          .withHost("localhost")
          .withPort(5002)
          .build())
        .addMember(NettyMember.builder()
          .withId(3)
          .withHost("localhost")
          .withPort(5003)
          .build())
        .build())
      .withStateMachine(new TestStateMachine())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();

    Raft raft2 = Raft.builder()
      .withCluster(NettyCluster.builder()
        .withMemberId(2)
        .withHost("localhost")
        .withPort(5002)
        .addMember(NettyMember.builder()
          .withId(1)
          .withHost("localhost")
          .withPort(5001)
          .build())
        .addMember(NettyMember.builder()
          .withId(3)
          .withHost("localhost")
          .withPort(5003)
          .build())
        .build())
      .withStateMachine(new TestStateMachine())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();

    Raft raft3 = Raft.builder()
      .withCluster(NettyCluster.builder()
        .withMemberId(3)
        .withHost("localhost")
        .withPort(5003)
        .addMember(NettyMember.builder()
          .withId(1)
          .withHost("localhost")
          .withPort(5001)
          .build())
        .addMember(NettyMember.builder()
          .withId(2)
          .withHost("localhost")
          .withPort(5002)
          .build())
        .build())
      .withStateMachine(new TestStateMachine())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();

    expectResumes(3);
    raft1.open().thenRun(this::resume);
    raft2.open().thenRun(this::resume);
    raft3.open().thenRun(this::resume);
    await();

    Raft client = Raft.builder()
      .withCluster(NettyCluster.builder()
        .withMemberId(4)
        .withHost("localhost")
        .withPort(5004)
        .addMember(NettyMember.builder()
          .withId(1)
          .withHost("localhost")
          .withPort(5001)
          .build())
        .addMember(NettyMember.builder()
          .withId(2)
          .withHost("localhost")
          .withPort(5002)
          .build())
        .addMember(NettyMember.builder()
          .withId(3)
          .withHost("localhost")
          .withPort(5003)
          .build())
        .build())
      .withStateMachine(new TestStateMachine())
      .withLog(Log.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .build())
      .build();

    client.open().join();

    while (true);
  }

  /**
   * Test state machine.
   */
  public static class TestStateMachine extends StateMachine {

  }

}
