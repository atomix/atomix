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
import net.kuujo.copycat.cluster.internal.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatorConfig;
import net.kuujo.copycat.cluster.internal.coordinator.DefaultClusterCoordinator;
import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import org.testng.annotations.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
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
    ClusterCoordinator coordinator = new DefaultClusterCoordinator(new CoordinatorConfig().withName("test").withClusterConfig(cluster));
    return coordinator.<TestResource>getResource("test", new TestResource.Config().withLog(new BufferedLog()).resolve(cluster))
      .addStartupTask(() -> coordinator.open().thenApply(v -> null))
      .addShutdownTask(coordinator::close);
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
      if (event.member().uri().equals(passive.cluster().member().uri())) {
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
      if (event.member().uri().equals(passive2.cluster().member().uri())) {
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
      if (event.type() == MembershipEvent.Type.JOIN && event.member().uri().equals(passive.cluster().member().uri())) {
        threadAssertTrue(joined.compareAndSet(false, true));
      } else if (event.type() == MembershipEvent.Type.LEAVE && event.member().uri().equals(passive.cluster().member().uri())) {
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
      if (event.type() == MembershipEvent.Type.JOIN && event.member().uri().equals(passive2.cluster().member().uri())) {
        threadAssertTrue(joined.compareAndSet(false, true));
      } else if (event.type() == MembershipEvent.Type.LEAVE && event.member().uri().equals(passive2.cluster().member().uri())) {
        threadAssertTrue(joined.get());
        resume();
      }
    });

    test.open().thenRun(passive2::close);
    await(10000);
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

    server.handler(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      threadAssertEquals(new String(bytes), "Hello world!");
      return CompletableFuture.completedFuture(ByteBuffer.wrap("Hello world back!".getBytes()));
    });
    server.listen().thenRunAsync(this::resume);
    await(5000);

    client.connect().thenRunAsync(this::resume);
    await(5000);

    client.write(ByteBuffer.wrap("Hello world!".getBytes())).thenAcceptAsync(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      threadAssertEquals(new String(bytes), "Hello world back!");
      resume();
    });
    await(5000);

    client.write(ByteBuffer.wrap("Hello world!".getBytes())).thenAcceptAsync(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      threadAssertEquals(new String(bytes), "Hello world back!");
      resume();
    });
    await(5000);

    client.write(ByteBuffer.wrap("Hello world!".getBytes())).thenAcceptAsync(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      threadAssertEquals(new String(bytes), "Hello world back!");
      resume();
    });
    await(5000);
  }

}
