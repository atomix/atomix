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
import io.atomix.group.task.TaskFailedException;
import io.atomix.testing.AbstractCopycatTest;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

    AtomicBoolean joined = new AtomicBoolean();
    group2.join().join();
    group2.onJoin(member -> {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      joined.set(true);
      resume();
    });

    group1.join().thenRun(() -> {
      threadAssertTrue(joined.get());
      resume();
    });

    await(0, 2);
  }

  /**
   * Tests leaving a group.
   */
  public void testLeave() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    LocalMember localMember = group2.join().get();
    assertEquals(group2.members().size(), 1);

    group1.join().thenRun(() -> {
      threadAssertEquals(group1.members().size(), 2);
      threadAssertEquals(group2.members().size(), 2);
      resume();
    });

    await(5000);

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

    LocalMember localMember = group2.join().get();
    assertEquals(group2.members().size(), 1);

    group1.join().thenRun(() -> {
      threadAssertEquals(group1.members().size(), 2);
      threadAssertEquals(group2.members().size(), 2);
      resume();
    });

    await(5000);

    group1.onLeave(member -> {
      threadAssertEquals(member.id(), localMember.id());
      resume();
    });
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

    group2.join("2").thenRun(this::resume);
    await(5000);

    assertEquals(group2.members().size(), 1);

    group1.join("1").thenRun(() -> {
      threadAssertEquals(group1.members().size(), 2);
      threadAssertEquals(group2.members().size(), 2);
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
   * Tests electing a group leader.
   */
  public void testElectLeave() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    LocalMember localMember2 = group2.join().get();
    assertEquals(group2.members().size(), 1);
    assertEquals(group2.election().term().leader(), localMember2);

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
    assertEquals(group2.members().size(), 1);
    assertEquals(group2.election().term().leader(), localMember2);

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
   * Tests setting and getting member properties.
   */
  public void testProperties() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    LocalMember localMember = group1.join().get();
    localMember.properties().set("foo", "Hello world!").thenRun(this::resume);

    await(5000);

    assertEquals(group2.members().size(), 1);
    group2.members().iterator().next().properties().get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(5000);

    localMember.properties().remove("foo").thenRun(this::resume);
    await(5000);

    assertEquals(group2.members().size(), 1);
    group2.members().iterator().next().properties().get("foo").thenAccept(result -> {
      threadAssertNull(result);
      resume();
    });

    await(5000);
  }

  /**
   * Tests setting and getting member properties.
   */
  public void testPersistentProperties() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    String memberId = UUID.randomUUID().toString();

    LocalMember localMember1 = group1.join(memberId).get();

    localMember1.properties().set("foo", "Hello world!").thenRun(this::resume);
    await(5000);

    assertEquals(group2.members().size(), 1);
    group2.members().iterator().next().properties().get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(5000);

    group1.close().thenRun(this::resume);
    await(5000);

    LocalMember localMember2 = group2.join(memberId).get();

    assertEquals(group2.members().size(), 1);
    localMember2.properties().get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(5000);
  }

  /**
   * Tests sending message between members.
   */
  public void testDirectMessage() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6000)));
    DistributedGroup group2 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6001)));

    group1.join().thenAccept(member -> {
      member.messages().consumer("foo").onMessage(message -> {
        threadAssertEquals(message.body(), "Hello world!");
        message.reply("bar");
        resume();
      });
      resume();
    });

    await(5000);

    assertEquals(group2.members().size(), 1);
    group2.members().iterator().next().messages().producer("foo").send("Hello world!").thenAccept(result -> {
      threadAssertEquals(result, "bar");
      resume();
    });

    await(10000, 2);
  }

  /**
   * Tests direct member tasks.
   */
  public void testDirectTask() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6000)));
    DistributedGroup group2 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6001)));

    LocalMember member = group2.join().get(10, TimeUnit.SECONDS);

    assertEquals(group1.members().size(), 1);
    assertEquals(group2.members().size(), 1);

    member.tasks().consumer("test").onTask(task -> {
      threadAssertEquals(task.task(), "Hello world!");
      task.ack();
      resume();
    });
    group1.member(member.id()).tasks().producer("test").submit("Hello world!").thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests failing a direct task.
   */
  public void testDirectTaskFail() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6000)));
    DistributedGroup group2 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6001)));

    LocalMember member = group2.join().get(10, TimeUnit.SECONDS);

    assertEquals(group1.members().size(), 1);
    assertEquals(group2.members().size(), 1);

    member.tasks().consumer("test").onTask(task -> {
      threadAssertEquals(task.task(), "Hello world!");
      task.fail();
      resume();
    });
    group1.member(member.id()).tasks().producer("test").submit("Hello world!").whenComplete((result, error) -> {
      threadAssertTrue(error instanceof TaskFailedException);
      resume();
    });
    await(10000, 2);
  }

  /**
   * Tests that a task is failed when a member leaves before the task is processed.
   */
  public void testDirectTaskFailOnLeave() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6000)));
    DistributedGroup group2 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6001)));

    LocalMember member = group2.join().get(10, TimeUnit.SECONDS);

    assertEquals(group1.members().size(), 1);
    assertEquals(group2.members().size(), 1);

    member.tasks().consumer("test").onTask(task -> {
      member.leave().thenRun(this::resume);
      resume();
    });

    group1.member(member.id()).tasks().producer("test").submit("Hello world!").whenComplete((result, error) -> {
      threadAssertTrue(error instanceof TaskFailedException);
      resume();
    });
    await(10000, 3);
  }

  /**
   * Tests fan-out member tasks.
   */
  public void testAllTask() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6000)));
    DistributedGroup group2 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6001)));

    LocalMember member1 = group1.join().get(10, TimeUnit.SECONDS);
    LocalMember member2 = group2.join().get(10, TimeUnit.SECONDS);
    LocalMember member3 = group2.join().get(10, TimeUnit.SECONDS);

    assertEquals(group1.members().size(), 3);
    assertEquals(group2.members().size(), 3);

    member1.tasks().consumer("test").onTask(task -> {
      threadAssertEquals(task.task(), "Hello world!");
      task.ack();
      resume();
    });
    member2.tasks().consumer("test").onTask(task -> {
      threadAssertEquals(task.task(), "Hello world!");
      task.ack();
      resume();
    });
    member3.tasks().consumer("test").onTask(task -> {
      threadAssertEquals(task.task(), "Hello world!");
      task.ack();
      resume();
    });
    group1.tasks().producer("test").submit("Hello world!").thenRun(this::resume);
    await(10000, 4);
  }

}
