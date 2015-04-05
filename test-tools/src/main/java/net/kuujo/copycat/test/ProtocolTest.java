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
package net.kuujo.copycat.test;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.MembershipEvent;
import net.kuujo.copycat.io.HeapBuffer;
import net.kuujo.copycat.log.LogConfig;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base protocol test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public abstract class ProtocolTest extends ConcurrentTestCase {

  /**
   * Creates a test protocol.
   */
  protected abstract Protocol createProtocol();

  /**
   * Creates a test URI.
   */
  protected abstract String createUri(int id);

  /**
   * Creates a new test resource.
   */
  private TestResource createTestResource(ClusterConfig cluster) {
    return new TestResource(new ResourceContext(new TestResource.Config().withLog(new LogConfig()), cluster, Executors
      .newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-test-%d"))));
  }

  /**
   * Creates a new test cluster.
   */
  private TestCluster<TestResource> createTestCluster() {
    return TestCluster.<TestResource>builder()
      .withActiveMembers(3)
      .withPassiveMembers(2)
      .withUriFactory(this::createUri)
      .withClusterFactory(members -> new ClusterConfig().withProtocol(createProtocol()).withMembers(members))
      .withResourceFactory(this::createTestResource)
      .build();
  }

  /**
   * Tests a member joining on an active member of the cluster.
   */
  public void testClusterJoinActiveMember() throws Throwable {
    TestCluster<TestResource> test = createTestCluster();
    TestResource active = test.activeResources().iterator().next();
    TestResource passive = test.passiveResources().iterator().next();

    expectResume();
    Cluster cluster = active.cluster();
    cluster.addMembershipListener(event -> {
      if (event.member().address().equals(passive.cluster().member().address())) {
        threadAssertTrue(event.type() == MembershipEvent.Type.JOIN);
        resume();
      }
    });

    test.open();
    await();
    test.close().get();
  }

  /**
   * Tests a member joining on a passive member of the cluster.
   */
  public void testClusterJoinPassiveMember() throws Throwable {
    TestCluster<TestResource> test = createTestCluster();
    Iterator<TestResource> iterator = test.passiveResources().iterator();
    TestResource passive1 = iterator.next();
    TestResource passive2 = iterator.next();

    expectResume();
    Cluster cluster = passive1.cluster();
    cluster.addMembershipListener(event -> {
      if (event.member().address().equals(passive2.cluster().member().address())) {
        threadAssertTrue(event.type() == MembershipEvent.Type.JOIN);
        resume();
      }
    });

    test.open();
    await();
    test.close().get();
  }

  /**
   * Tests a member leaving on an active member of the cluster.
   */
  public void testClusterLeaveActiveMember() throws Throwable {
    TestCluster<TestResource> test = createTestCluster();
    TestResource active = test.activeResources().iterator().next();
    TestResource passive = test.passiveResources().iterator().next();

    AtomicBoolean joined = new AtomicBoolean();
    expectResume();
    Cluster cluster = active.cluster();
    cluster.addMembershipListener(event -> {
      if (event.type() == MembershipEvent.Type.JOIN && event.member().address().equals(passive.cluster().member().address())) {
        threadAssertTrue(joined.compareAndSet(false, true));
      } else if (event.type() == MembershipEvent.Type.LEAVE && event.member().address().equals(passive.cluster().member().address())) {
        threadAssertTrue(joined.get());
        resume();
      }
    });

    test.open().thenRun(passive::close);
    await(10000);
    test.close().get();
  }

  /**
   * Tests a member leaving on a passive member of the cluster.
   */
  public void testClusterLeavePassiveMember() throws Throwable {
    TestCluster<TestResource> test = createTestCluster();
    Iterator<TestResource> iterator = test.passiveResources().iterator();
    TestResource passive1 = iterator.next();
    TestResource passive2 = iterator.next();

    AtomicBoolean joined = new AtomicBoolean();
    expectResume();
    Cluster cluster = passive1.cluster();
    cluster.addMembershipListener(event -> {
      if (event.type() == MembershipEvent.Type.JOIN && event.member().address().equals(passive2.cluster().member().address())) {
        threadAssertTrue(joined.compareAndSet(false, true));
      } else if (event.type() == MembershipEvent.Type.LEAVE && event.member().address().equals(passive2.cluster().member().address())) {
        threadAssertTrue(joined.get());
        resume();
      }
    });

    test.open().thenRun(passive2::close);
    await(15000);
    test.close().get();
  }

  /**
   * Tests sending from a client to sever and back.
   */
  public void testSendReceive() throws Throwable {
    Protocol protocol = createProtocol();
    String uri = createUri(1);
    ProtocolServer server = protocol.createServer(new URI(uri));
    ProtocolClient client = protocol.createClient(new URI(uri));

    server.connectListener(connection -> {
      connection.handler(buffer -> {
        byte[] bytes = new byte[(int) buffer.remaining()];
        buffer.read(bytes);
        threadAssertEquals(new String(bytes), "Hello world!");
        return CompletableFuture.completedFuture(HeapBuffer.allocate("Hello world back!".getBytes().length).write("Hello world back!".getBytes()).flip());
      });
    });

    server.listen().thenRunAsync(this::resume);
    await(5000);

    client.connect().thenRunAsync(this::resume);
    await(5000);

    expectResumes(3);
    client.connect().thenAccept(connection -> {
      connection.write(HeapBuffer.allocate("Hello world!".getBytes().length).write("Hello world!".getBytes()).flip()).thenAcceptAsync(buffer -> {
        byte[] bytes = new byte[(int) buffer.remaining()];
        buffer.read(bytes);
        threadAssertEquals(new String(bytes), "Hello world back!");
        resume();
      });

      connection.write(HeapBuffer.allocate("Hello world!".getBytes().length).write("Hello world!".getBytes()).flip()).thenAcceptAsync(buffer -> {
        byte[] bytes = new byte[(int) buffer.remaining()];
        buffer.read(bytes);
        threadAssertEquals(new String(bytes), "Hello world back!");
        resume();
      });

      connection.write(HeapBuffer.allocate("Hello world!".getBytes().length).write("Hello world!".getBytes()).flip()).thenAcceptAsync(buffer -> {
        byte[] bytes = new byte[(int) buffer.remaining()];
        buffer.read(bytes);
        threadAssertEquals(new String(bytes), "Hello world back!");
        resume();
      });
    });
    await(5000);

    expectResume();
    client.close().thenRunAsync(this::resume);
    await(2500);

    expectResume();
    server.close().thenRunAsync(this::resume);
    await(2500);
  }

}
