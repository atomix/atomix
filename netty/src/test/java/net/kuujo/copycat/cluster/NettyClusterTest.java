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

import io.netty.channel.nio.NioEventLoopGroup;
import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.Writable;
import net.kuujo.copycat.util.ExecutionContext;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import static org.testng.Assert.assertEquals;

/**
 * Netty cluster test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class NettyClusterTest extends ConcurrentTestCase {

  /**
   * Tests connecting a remote member to a local member.
   */
  public void testConnectRemoteToLocal() throws Throwable {
    NettyLocalMember localMember = new NettyLocalMember(new NettyMember.Info(1, Member.Type.ACTIVE, new InetSocketAddress("localhost", 8080)), new Serializer(), new ExecutionContext("test-server"));

    expectResume();
    localMember.listen().thenRun(this::resume);
    await();

    NettyRemoteMember remoteMember = new NettyRemoteMember(new NettyMember.Info(1, Member.Type.ACTIVE, new InetSocketAddress("localhost", 8080)), new ExecutionContext("test-client"));
    remoteMember.setSerializer(new Serializer());
    remoteMember.setEventLoopGroup(new NioEventLoopGroup());

    expectResume();
    remoteMember.connect().thenRun(this::resume);
    await();
  }

  /**
   * Tests connecting a remote member to a local member.
   */
  public void testConnectRemoteBeforeLocal() throws Throwable {
    NettyRemoteMember remoteMember = new NettyRemoteMember(new NettyMember.Info(1, Member.Type.ACTIVE, new InetSocketAddress("localhost", 8080)), new ExecutionContext("test-client"));
    remoteMember.setSerializer(new Serializer());
    remoteMember.setEventLoopGroup(new NioEventLoopGroup());

    expectResumes(2);
    remoteMember.connect().thenRun(this::resume);

    NettyLocalMember localMember = new NettyLocalMember(new NettyMember.Info(1, Member.Type.ACTIVE, new InetSocketAddress("localhost", 8080)), new Serializer(), new ExecutionContext("test-server"));

    localMember.listen().thenRun(this::resume);
    await();
  }

  /**
   * Tests sending a message between remote and local members.
   */
  public void testMessageRemoteToLocal() throws Throwable {
    NettyLocalMember localMember = new NettyLocalMember(new NettyMember.Info(1, Member.Type.ACTIVE, new InetSocketAddress("localhost", 8080)), new Serializer(), new ExecutionContext("test-server"));

    expectResume();
    localMember.listen().thenRun(this::resume);
    await();

    NettyRemoteMember remoteMember = new NettyRemoteMember(new NettyMember.Info(1, Member.Type.ACTIVE, new InetSocketAddress("localhost", 8080)), new ExecutionContext("test-client"));
    remoteMember.setSerializer(new Serializer());
    remoteMember.setEventLoopGroup(new NioEventLoopGroup());

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
   * Tests executing a task between remote and local members.
   */
  public void testTaskRemoteToLocal() throws Throwable {
    NettyLocalMember localMember = new NettyLocalMember(new NettyMember.Info(1, Member.Type.ACTIVE, new InetSocketAddress("localhost", 8080)), new Serializer().register(TestTask.class, 1), new ExecutionContext("test-server"));

    expectResume();
    localMember.listen().thenRun(this::resume);
    await();

    NettyRemoteMember remoteMember = new NettyRemoteMember(new NettyMember.Info(1, Member.Type.ACTIVE, new InetSocketAddress("localhost", 8080)), new ExecutionContext("test-client"));
    remoteMember.setSerializer(new Serializer().register(TestTask.class, 1));
    remoteMember.setEventLoopGroup(new NioEventLoopGroup());

    expectResume();
    remoteMember.connect().thenRun(this::resume);
    await();

    expectResume();
    localMember.registerHandler("test", message -> CompletableFuture.completedFuture("world!"));
    remoteMember.submit(new TestTask("Hello")).whenComplete((result, error) -> {
      threadAssertNull(error);
      threadAssertEquals(result, "world!");
      resume();
    });
    await();
  }

  /**
   * Tests sending and receiving messages across a cluster.
   */
  public void testClusterMessage() throws Throwable {
    ManagedCluster cluster1 = buildCluster(1, 3);
    ManagedCluster cluster2 = buildCluster(2, 3);
    ManagedCluster cluster3 = buildCluster(3, 3);

    expectResumes(3);

    cluster1.open().thenRun(this::resume);
    cluster2.open().thenRun(this::resume);
    cluster3.open().thenRun(this::resume);

    await();

    assertEquals(cluster1.member().id(), 1);
    assertEquals(cluster2.member().id(), 2);
    assertEquals(cluster3.member().id(), 3);

    assertEquals(cluster1.member().type(), Member.Type.ACTIVE);
    assertEquals(cluster2.member().type(), Member.Type.ACTIVE);
    assertEquals(cluster3.member().type(), Member.Type.ACTIVE);

    cluster1.member().<String, String>registerHandler("test", message -> {
      threadAssertEquals(message, "Hello");
      return CompletableFuture.completedFuture("world!");
    });

    expectResume();

    cluster2.member(1).send("test", "Hello").whenComplete((result, error) -> {
      threadAssertNull(error);
      threadAssertEquals(result, "world!");
      resume();
    });

    await();
  }

  /**
   * Tests joining the cluster as a passive member.
   */
  public void testClusterJoin() throws Throwable {
    ManagedCluster cluster1 = buildCluster(1, 3);
    ManagedCluster cluster2 = buildCluster(2, 3);
    ManagedCluster cluster3 = buildCluster(3, 3);

    expectResumes(3);

    cluster1.open().thenRun(this::resume);
    cluster2.open().thenRun(this::resume);
    cluster3.open().thenRun(this::resume);

    await();

    cluster1.addMembershipListener(event -> {
      threadAssertEquals(event.type(), MembershipChangeEvent.Type.JOIN);
      threadAssertEquals(event.member().id(), 4);
      resume();
    });

    ManagedCluster cluster4 = NettyCluster.builder()
      .withMemberId(4)
      .withMemberType(Member.Type.PASSIVE)
      .withHost("localhost")
      .withPort(8080 + 4)
      .addSeed(NettyMember.builder()
        .withId(1)
        .withHost("localhost")
        .withPort(8081)
        .build())
      .addSeed(NettyMember.builder()
        .withId(2)
        .withHost("localhost")
        .withPort(8082)
        .build())
      .addSeed(NettyMember.builder()
        .withId(3)
        .withHost("localhost")
        .withPort(8083)
        .build())
      .build();

    expectResumes(2);

    cluster4.open().thenRun(this::resume);

    await();
  }

  /**
   * Builds a cluster.
   */
  private ManagedCluster buildCluster(int localMember, int members) {
    NettyCluster.Builder builder = NettyCluster.builder()
      .withMemberId(localMember)
      .withHost("localhost")
      .withPort(8080 + localMember);

    for (int i = 1; i <= members; i++) {
      if (i != localMember) {
        builder.addSeed(NettyMember.builder()
          .withId(i)
          .withHost("localhost")
          .withPort(8080 + i)
          .build());
      }
    }
    return builder.build();
  }

  /**
   * Test task.
   */
  public static class TestTask implements Task<String>, Writable {
    private String arg;

    public TestTask() {
    }

    public TestTask(String arg) {
      this.arg = arg;
    }

    @Override
    public String execute() {
      return "world!";
    }

    @Override
    public void writeObject(Buffer buffer) {
      byte[] bytes = arg.getBytes();
      buffer.writeInt(bytes.length).write(bytes);
    }

    @Override
    public void readObject(Buffer buffer) {
      byte[] bytes = new byte[buffer.readInt()];
      buffer.read(bytes);
      this.arg = new String(bytes);
    }
  }

}
