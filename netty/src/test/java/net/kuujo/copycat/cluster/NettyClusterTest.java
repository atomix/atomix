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

import io.netty.channel.EventLoopGroup;
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
    NettyLocalMember localMember = new NettyLocalMember(new NettyMemberInfo(1, new InetSocketAddress("localhost", 8080)), Member.Type.ACTIVE);
    localMember.setContext(new ExecutionContext("test-server", new Serializer()));

    expectResume();
    localMember.listen().thenRun(this::resume);
    await();

    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyRemoteMember remoteMember = new NettyRemoteMember(new NettyMemberInfo(1, new InetSocketAddress("localhost", 8080)), Member.Type.ACTIVE);
    remoteMember.setContext(new ExecutionContext("test-client", new Serializer()));
    remoteMember.setEventLoopGroup(eventLoopGroup);

    expectResume();
    remoteMember.connect().thenRun(this::resume);
    await();

    expectResumes(2);

    localMember.close().thenRun(this::resume);
    remoteMember.close().thenRun(this::resume);

    await();

    eventLoopGroup.shutdownGracefully();
  }

  /**
   * Tests connecting a remote member to a local member.
   */
  public void testConnectRemoteBeforeLocal() throws Throwable {
    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyRemoteMember remoteMember = new NettyRemoteMember(new NettyMemberInfo(1, new InetSocketAddress("localhost", 8081)), Member.Type.ACTIVE);
    remoteMember.setContext(new ExecutionContext("test-client", new Serializer()));
    remoteMember.setEventLoopGroup(eventLoopGroup);

    expectResumes(2);
    remoteMember.connect().thenRun(this::resume);

    NettyLocalMember localMember = new NettyLocalMember(new NettyMemberInfo(1, new InetSocketAddress("localhost", 8081)), Member.Type.ACTIVE);
    localMember.setContext(new ExecutionContext("test-server", new Serializer()));

    localMember.listen().thenRun(this::resume);
    await();

    expectResumes(2);

    localMember.close().thenRun(this::resume);
    remoteMember.close().thenRun(this::resume);

    await();

    eventLoopGroup.shutdownGracefully();
  }

  /**
   * Tests sending a message between remote and local members.
   */
  public void testMessageRemoteToLocal() throws Throwable {
    NettyLocalMember localMember = new NettyLocalMember(new NettyMemberInfo(1, new InetSocketAddress("localhost", 8082)), Member.Type.ACTIVE);
    localMember.setContext(new ExecutionContext("test-server", new Serializer()));

    expectResume();
    localMember.listen().thenRun(this::resume);
    await();

    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyRemoteMember remoteMember = new NettyRemoteMember(new NettyMemberInfo(1, new InetSocketAddress("localhost", 8082)), Member.Type.ACTIVE);
    remoteMember.setContext(new ExecutionContext("test-client", new Serializer()));
    remoteMember.setEventLoopGroup(eventLoopGroup);

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

    expectResumes(2);

    localMember.close().thenRun(this::resume);
    remoteMember.close().thenRun(this::resume);

    await();

    eventLoopGroup.shutdownGracefully();
  }

  /**
   * Tests executing a task between remote and local members.
   */
  public void testTaskRemoteToLocal() throws Throwable {
    NettyLocalMember localMember = new NettyLocalMember(new NettyMemberInfo(1, new InetSocketAddress("localhost", 8083)), Member.Type.ACTIVE);
    localMember.setContext(new ExecutionContext("test-server", new Serializer()));

    expectResume();
    localMember.listen().thenRun(this::resume);
    await();

    EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    NettyRemoteMember remoteMember = new NettyRemoteMember(new NettyMemberInfo(1, new InetSocketAddress("localhost", 8083)), Member.Type.ACTIVE);
    remoteMember.setContext(new ExecutionContext("test-client", new Serializer()));
    remoteMember.setEventLoopGroup(eventLoopGroup);

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

    expectResumes(2);

    remoteMember.close().thenRun(this::resume);
    localMember.close().thenRun(this::resume);

    await();

    eventLoopGroup.shutdownGracefully();
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

    expectResumes(3);

    cluster1.close().thenRun(this::resume);
    cluster2.close().thenRun(this::resume);
    cluster3.close().thenRun(this::resume);

    await();
  }

  /**
   * Builds a cluster.
   */
  private ManagedCluster buildCluster(int localMember, int members) {
    NettyCluster.Builder builder = NettyCluster.builder()
      .withMemberId(localMember)
      .withHost("localhost")
      .withPort(8090 + localMember);

    for (int i = 1; i <= members; i++) {
      builder.addMember(NettyMember.builder()
        .withId(i)
        .withHost("localhost")
        .withPort(8090 + i)
        .build());
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
    public void writeObject(Buffer buffer, Serializer serializer) {
      byte[] bytes = arg.getBytes();
      buffer.writeInt(bytes.length).write(bytes);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      byte[] bytes = new byte[buffer.readInt()];
      buffer.read(bytes);
      this.arg = new String(bytes);
    }
  }

}
