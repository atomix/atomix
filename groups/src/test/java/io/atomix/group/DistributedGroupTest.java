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
package io.atomix.group;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.session.ClientSession;
import io.atomix.group.messaging.MessageFailedException;
import io.atomix.group.messaging.MessageProducer;
import io.atomix.testing.AbstractCopycatTest;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

/**
 * Distributed group test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedGroupTest extends AbstractCopycatTest<DistributedGroup> {
  
  @Override
  protected Class<? super DistributedGroup> type() {
    return DistributedGroup.class;
  }

  /**
   * Tests joining a group.
   */
  public void testJoin() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    group1.onJoin(member -> {
      threadAssertEquals(member.id(), "test");
      resume();
    });

    group2.join("test").thenRun(this::resume);

    await(10000, 2);
  }

  /**
   * Tests leaving a group.
   */
  public void testLeave() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    group1.onJoin(m -> resume());
    group2.onJoin(m -> resume());

    LocalMember localMember = group2.join().get();

    await(5000, 2);

    group1.onLeave(member -> resume());
    localMember.leave().thenRun(this::resume);

    await(10000, 2);
  }

  /**
   * Tests a member being expired from the group.
   */
  public void testExpireLeave() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    group1.onJoin(m -> resume());
    group2.onJoin(m -> resume());

    group2.join().join();

    await(5000, 2);

    group1.onLeave(member -> resume());
    group2.close().thenRun(this::resume);

    await(10000, 2);
  }

  /**
   * Tests a persistent member being expired.
   */
  public void testPersistentExpire() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    group2.onJoin(m -> {
      if (group2.members().size() == 1) {
        resume();
      }
    });

    group2.join("2").thenRun(this::resume);
    await(5000, 2);

    group1.join("1").thenRun(() -> {
      threadAssertEquals(group1.members().size(), 2);
      resume();
    });

    await(5000);

    group1.onLeave(member -> {
      threadAssertEquals(member.id(), "2");
      resume();
    });
    group2.close().thenRun(this::resume);

    await(10000, 2);

    group1.onJoin(member -> {
      threadAssertEquals(member.id(), "2");
      resume();
    });

    DistributedGroup group3 = createResource();
    group3.join("2").thenRun(this::resume);

    await(10000);
  }

  /**
   * Tests that a new leader is elected when a persistent member is expired.
   */
  public void testPersistentExpireElectLeader() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    group1.election().onElection(term -> {
      if (term.leader().id().equals("a")) {
        group1.close().thenRun(this::resume);
      }
    });
    group2.election().onElection(term -> {
      if (term.leader().id().equals("b")) {
        resume();
      }
    });

    group1.join("a").join();
    group2.join("b").join();

    await(5000, 2);
  }

  /**
   * Tests that a persistent member is removed from the members list.
   */
  public void testPersistentMemberLeave() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    group1.onJoin(member -> {
      if (group1.members().size() == 2) {
        resume();
      }
    });
    group2.onJoin(member -> {
      if (group2.members().size() == 2) {
        resume();
      }
    });

    group1.join("a").join();
    group2.join("b").join();

    await(5000, 2);

    group2.onLeave(member -> {
      threadAssertEquals(group2.members().size(), 1);
      resume();
    });

    group1.close().thenRun(this::resume);

    await(5000, 2);
  }

  /**
   * Tests electing a group leader.
   */
  public void testElectLeave() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    LocalMember localMember2 = group2.join().get();
    group2.election().onElection(term -> {
      if (term.leader().equals(localMember2)) {
        resume();
      }
    });

    await(5000);

    LocalMember localMember1 = group1.join().get();
    group1.election().onElection(term -> {
      if (term.leader().equals(localMember1)) {
        resume();
      }
    });

    localMember2.leave().thenRun(this::resume);

    await(10000, 2);
  }

  /**
   * Tests electing a group leader.
   */
  public void testElectClose() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();


    LocalMember localMember2 = group2.join().get();
    group2.election().onElection(term -> {
      if (term.leader().equals(localMember2)) {
        resume();
      }
    });

    await(5000);

    LocalMember localMember1 = group1.join().get();
    group1.election().onElection(term -> {
      if (term.leader().equals(localMember1)) {
        resume();
      }
    });

    group2.close().thenRun(this::resume);

    await(10000, 2);
  }

  /**
   * Tests that a leader exists when a new group instance is created.
   */
  public void testLeaderOnJoin() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    LocalMember member1 = group1.join().join();
    group1.election().onElection(term -> {
      if (term.leader().equals(member1)) {
        resume();
      }
    });
    await(10000);

    DistributedGroup group2 = createResource();
    assertEquals(group2.election().term().term(), group1.election().term().term());
    assertEquals(group2.election().term().leader().id(), group1.election().term().leader().id());
  }

  /**
   * Tests sending a blocking message on a join event.
   */
  public void testBlockingMessageOnJoin() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options());
    DistributedGroup group2 = createResource(new DistributedGroup.Options());

    group1.onJoin(m -> {
      threadAssertEquals(group1.members().size(), 1);
      m.messaging().producer("test").send("Hello world!").join();
      resume();
    });

    LocalMember member = group2.join().get(10, TimeUnit.SECONDS);

    member.messaging().consumer("test").onMessage(message -> {
      threadAssertEquals(message.message(), "Hello world!");
      message.ack();
      resume();
    });

    await(5000, 2);
  }

  /**
   * Tests direct member messages.
   */
  public void testDirectMessage() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options());
    DistributedGroup group2 = createResource(new DistributedGroup.Options());

    group1.onJoin(m -> {
      threadAssertEquals(group1.members().size(), 1);
      resume();
    });
    group2.onJoin(m -> {
      threadAssertEquals(group2.members().size(), 1);
      resume();
    });

    LocalMember member = group2.join().get(10, TimeUnit.SECONDS);

    await(5000, 2);

    member.messaging().consumer("test").onMessage(message -> {
      threadAssertEquals(message.message(), "Hello world!");
      message.ack();
      resume();
    });
    group1.member(member.id()).messaging().producer("test").send("Hello world!").thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests failing a direct message.
   */
  public void testDirectMessageFail() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options());
    DistributedGroup group2 = createResource(new DistributedGroup.Options());

    group1.onJoin(m -> {
      threadAssertEquals(group1.members().size(), 1);
      resume();
    });
    group2.onJoin(m -> {
      threadAssertEquals(group2.members().size(), 1);
      resume();
    });

    LocalMember member = group2.join().get(10, TimeUnit.SECONDS);

    await(5000, 2);

    member.messaging().consumer("test").onMessage(message -> {
      threadAssertEquals(message.message(), "Hello world!");
      message.fail();
      resume();
    });
    group1.member(member.id()).messaging().producer("test").send("Hello world!").whenComplete((result, error) -> {
      threadAssertTrue(error instanceof MessageFailedException);
      resume();
    });
    await(10000, 2);
  }

  /**
   * Tests a direct request-reply message.
   */
  public void testDirectRequestReply() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options());
    DistributedGroup group2 = createResource(new DistributedGroup.Options());

    group1.onJoin(m -> {
      threadAssertEquals(group1.members().size(), 1);
      resume();
    });
    group2.onJoin(m -> {
      threadAssertEquals(group2.members().size(), 1);
      resume();
    });

    LocalMember member = group2.join().get(10, TimeUnit.SECONDS);

    await(5000, 2);

    member.messaging().consumer("test").onMessage(message -> {
      threadAssertEquals(message.message(), "Hello world!");
      message.reply("Hello world back!");
      resume();
    });

    MessageProducer.Options options = new MessageProducer.Options()
      .withDelivery(MessageProducer.Delivery.DIRECT)
      .withExecution(MessageProducer.Execution.REQUEST_REPLY);
    group1.member(member.id()).messaging().producer("test", options).send("Hello world!").thenAccept(response -> {
      threadAssertEquals(response, "Hello world back!");
      resume();
    });
    await(10000, 2);
  }

  /**
   * Tests that a direct message is redelivered to a persistent member after it rejoins the group.
   */
  public void testDirectMessageRedeliverToPersistentMember() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options());
    DistributedGroup group2 = createResource(new DistributedGroup.Options());

    group1.onJoin(m -> {
      threadAssertEquals(group1.members().size(), 1);
      resume();
    });
    group2.onJoin(m -> {
      threadAssertEquals(group2.members().size(), 1);
      resume();
    });

    LocalMember member = group2.join("test").get(10, TimeUnit.SECONDS);

    await(5000, 2);

    member.messaging().consumer("test").onMessage(message -> {
      threadAssertEquals(message.message(), "Hello world!");
      group1.join("test").thenAccept(localMember -> {
        localMember.messaging().consumer("test").onMessage(m -> {
          threadAssertEquals(message.message(), "Hello world!");
          m.ack();
          resume();
        });
      });
      resume();
    });
    group1.member(member.id()).messaging().producer("test").send("Hello world!").thenRun(this::resume);
    await(10000, 3);
  }

  public void testMetadataGetsSubmitted() throws Throwable {
    createServers(3);

    final Address something = new Address("localhost", 1337);
    DistributedGroup group = createResource(new DistributedGroup.Options());

    group.onJoin(ignore -> {
      threadAssertEquals(group.members().size(), 1);
      group.members().forEach(member -> {
        threadAssertTrue(member.metadata().isPresent());
        final Address meta = member.<Address>metadata().get();
        threadAssertEquals(meta, something);
      });
      resume();
    });

    group.join(null, something).get(10, TimeUnit.SECONDS);

    await(5000, 1);
  }
  /**
   * Tests that a message is failed when a member leaves before the message is processed.
   */
  public void testDirectMessageFailOnLeave() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options());
    DistributedGroup group2 = createResource(new DistributedGroup.Options());

    group1.onJoin(m -> {
      threadAssertEquals(group1.members().size(), 1);
      resume();
    });
    group2.onJoin(m -> {
      threadAssertEquals(group2.members().size(), 1);
      resume();
    });

    LocalMember member = group2.join().get(10, TimeUnit.SECONDS);

    await(5000, 2);

    member.messaging().consumer("test").onMessage(message -> {
      member.leave().thenRun(this::resume);
      resume();
    });

    group1.member(member.id()).messaging().producer("test").send("Hello world!").whenComplete((result, error) -> {
      threadAssertTrue(error instanceof MessageFailedException);
      resume();
    });
    await(10000, 3);
  }

  /**
   * Tests fan-out member messages.
   */
  public void testGroupMessage() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options());
    DistributedGroup group2 = createResource(new DistributedGroup.Options());

    group1.onJoin(m -> {
      if (group1.members().size() == 3) {
        resume();
      }
    });
    group2.onJoin(m -> {
      if (group2.members().size() == 3) {
        resume();
      }
    });

    LocalMember member1 = group1.join().get(10, TimeUnit.SECONDS);
    LocalMember member2 = group2.join().get(10, TimeUnit.SECONDS);
    LocalMember member3 = group2.join().get(10, TimeUnit.SECONDS);

    member1.messaging().consumer("test").onMessage(message -> {
      threadAssertEquals(message.message(), "Hello world!");
      message.ack();
      resume();
    });
    member2.messaging().consumer("test").onMessage(message -> {
      threadAssertEquals(message.message(), "Hello world!");
      message.ack();
      resume();
    });
    member3.messaging().consumer("test").onMessage(message -> {
      threadAssertEquals(message.message(), "Hello world!");
      message.ack();
      resume();
    });
    group1.messaging().producer("test").send("Hello world!").thenRun(this::resume);
    await(10000, 4);
  }

  /**
   * Tests that a {@code ONCE} message is failed when a member leaves the group without acknowledging
   * the message.
   */
  public void testGroupMessageFailOnLeave() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options());
    DistributedGroup group2 = createResource(new DistributedGroup.Options());

    group1.onJoin(m -> {
      if (group1.members().size() == 2) {
        resume();
      }
    });
    group2.onJoin(m -> {
      if (group2.members().size() == 2) {
        resume();
      }
    });

    LocalMember member1 = group1.join().get(10, TimeUnit.SECONDS);
    LocalMember member2 = group2.join().get(10, TimeUnit.SECONDS);

    await(5000, 2);

    member1.messaging().consumer("test").onMessage(message -> {
      threadAssertEquals(message.message(), "Hello world!");
      member1.leave();
      resume();
    });
    member2.messaging().consumer("test").onMessage(message -> {
      threadAssertEquals(message.message(), "Hello world!");
      member2.leave();
      resume();
    });

    MessageProducer.Options options = new MessageProducer.Options()
      .withDelivery(MessageProducer.Delivery.RANDOM);
    group1.messaging().producer("test", options).send("Hello world!").whenComplete((result, error) -> {
      threadAssertNotNull(error);
      resume();
    });
    await(10000, 2);
  }

  /**
   * Tests that a member already exists when a join event is received.
   */
  public void testMemberExistsOnJoinEvent() throws Throwable {
    createServers(3);

    final CopycatClient client1 = createCopycatClient();
    final CopycatClient client2 = createCopycatClient();

    final DistributedGroup group1 = createResource(client1, new DistributedGroup.Options());
    final DistributedGroup group2 = createResource(client2, new DistributedGroup.Options());

    group1.onJoin(m -> {
      threadAssertEquals(1, group1.members().size());
      resume();
    });
    group2.onJoin(m -> {
      threadAssertEquals(1, group2.members().size());
      resume();
    });

    group1.join().thenRun(this::resume);

    await(5000, 3);
  }

  /**
   * Tests that the local onLeave handler is called when a member leaves the group.
   */
  public void testLocalOnLeave() throws Throwable {
    createServers(3);

    final CopycatClient client1 = createCopycatClient();
    final CopycatClient client2 = createCopycatClient();

    final DistributedGroup group1 = createResource(client1, new DistributedGroup.Options());
    final DistributedGroup group2 = createResource(client2, new DistributedGroup.Options());

    // Group join handlers
    group1.onJoin(m -> resume());
    group2.onJoin(m -> resume());

    // Join both members to the group
    final LocalMember member1 = group1.join().get(5, TimeUnit.SECONDS);
    final LocalMember member2 = group2.join().get(5, TimeUnit.SECONDS);

    await(5000, 4);

    group1.onLeave(m -> {
      resume();
    });
    group2.onLeave(m -> {
      resume();
    });

    member1.leave().thenRun(this::resume);

    await(5000, 3);
  }

  /**
   * Tests the recovery of a group resource/member in a group.
   */
  public void testRecovery() throws Throwable {
    createServers(3);

    final CopycatClient client1 = createCopycatClient();
    final CopycatClient client2 = createCopycatClient();

    final DistributedGroup group1 = createResource(client1, new DistributedGroup.Options());
    final DistributedGroup group2 = createResource(client2, new DistributedGroup.Options());

    // Group1 on join handler
    group1.onJoin(m -> {
      System.out.println(group1.members().size());
      if (group1.members().size() == 2) {
        resume();
      }
    });

    // Group2 on join handler
    group2.onJoin(m -> {
      System.out.println(group2.members().size());
      if (group2.members().size() == 2) {
        resume();
      }
    });

    // Join both members to the group
    final LocalMember member1 = group1.join().get(5, TimeUnit.SECONDS);
    final LocalMember member2 = group2.join().get(5, TimeUnit.SECONDS);

    // Wait for both group instances to receive join events for both members
    await(5000, 2);

    // Set a recovery handler for group1
    group1.onRecovery(attempt -> {
      threadAssertEquals(1, attempt);
      resume();
    });

    // Expire client 1's session, which should cause group1 to expire
    ((ClientSession) client1.session()).expire().whenComplete((v, e) -> {
      threadAssertNull(e);
      resume();
    });

    // Wait for the client's session to expire and be recovered
    await(5000, 4);

    // Ensure one member remains once a node is removed
    group1.onLeave(m -> {
      threadAssertEquals(1, group1.members().size());
      resume();
    });
    group2.onLeave(m -> {
      threadAssertEquals(1, group2.members().size());
      resume();
    });

    // Remove member2 from the group
    member2.leave().thenRun(this::resume);
    await(5000, 3);
  }

}
