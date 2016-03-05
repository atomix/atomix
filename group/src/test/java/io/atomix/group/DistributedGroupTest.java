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

    LocalGroupMember localMember = group2.join().get();
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

    LocalGroupMember localMember = group2.join().get();
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
  public void testElectResign() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    LocalGroupMember localMember2 = group2.join().get();
    assertEquals(group2.members().size(), 1);

    localMember2.onElection(term -> resume());

    await(5000);

    LocalGroupMember localMember1 = group1.join().get();
    localMember1.onElection(term -> resume());

    localMember2.resign().thenRun(this::resume);

    await(10000, 2);
  }

  /**
   * Tests electing a group leader.
   */
  public void testElectClose() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    LocalGroupMember localMember2 = group2.join().get();
    assertEquals(group2.members().size(), 1);

    localMember2.onElection(term -> resume());

    await(5000);

    LocalGroupMember localMember1 = group1.join().get();
    localMember1.onElection(term -> resume());

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

    LocalGroupMember localMember = group1.join().get();
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

    LocalGroupMember localMember1 = group1.join(memberId).get();

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

    LocalGroupMember localMember2 = group2.join(memberId).get();

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
      member.connection().onMessage("foo", message -> {
        threadAssertEquals(message.body(), "Hello world!");
        message.reply("bar");
        resume();
      });
      resume();
    });

    await(5000);

    assertEquals(group2.members().size(), 1);
    group2.members().iterator().next().connection().send("foo", "Hello world!").thenAccept(result -> {
      threadAssertEquals(result, "bar");
      resume();
    });

    await(10000, 2);
  }

  /**
   * Tests sending a message to a partition.
   */
  public void testPartitionMessage() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6000)));
    DistributedGroup group2 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6001)));

    LocalGroupMember member1 = group1.join().get(10, TimeUnit.SECONDS);
    LocalGroupMember member2 = group2.join().get(10, TimeUnit.SECONDS);
    LocalGroupMember member3 = group2.join().get(10, TimeUnit.SECONDS);

    assertEquals(group1.members().size(), 3);
    assertEquals(group2.members().size(), 3);

    member1.connection().onMessage("test", message -> {
      threadAssertEquals(message.body(), "Hello world!");
      message.reply("Hello world back!");
      resume();
    });
    member2.connection().onMessage("test", message -> {
      threadAssertEquals(message.body(), "Hello world!");
      message.reply("Hello world back!");
      resume();
    });
    member3.connection().onMessage("test", message -> {
      threadAssertEquals(message.body(), "Hello world!");
      message.reply("Hello world back!");
      resume();
    });

    PartitionGroup partitions = group1.partition(3);
    partitions.partitions().partition("Hello world!").members().iterator().next().connection().send("test", "Hello world!").thenAccept(reply -> {
      threadAssertEquals(reply, "Hello world back!");
      resume();
    });
    await(10000, 2);
  }

  /**
   * Tests partition migration.
   */
  public void testPartitionMigration() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6000)));
    DistributedGroup group2 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6001)));

    LocalGroupMember member1 = group1.join().get(10, TimeUnit.SECONDS);

    PartitionGroup partitions = group2.partition(3, 3);
    partitions.partitions().partition(0).onMigration(migration -> {
      threadAssertEquals(migration.source().id(), member1.id());
      threadAssertNotNull(migration.target());
      resume();
    });
    group2.join().thenRun(this::resume);
    await(5000, 2);
  }

  /**
   * Tests nested partitions.
   */
  public void testNestedPartitions() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6000)));
    group1.join("a").thenRun(this::resume);
    await(5000);

    DistributedGroup group2 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6001)));
    group2.join("b").thenRun(this::resume);
    await(5000);

    DistributedGroup group3 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6003)));
    group3.join("c").thenRun(this::resume);
    await(5000);

    assertEquals(group1.members().size(), 3);

    PartitionGroup partitions1 = group1.partition(3);
    PartitionGroup partitions2 = group2.partition(3);
    PartitionGroup partitions3 = group3.partition(3);

    assertEquals(partitions1.members(), partitions2.members());
    assertEquals(partitions2.members(), partitions3.members());

    PartitionGroup nestedPartitions1 = partitions1.partition(2);
    PartitionGroup nestedPartitions2 = partitions2.partition(2);
    PartitionGroup nestedPartitions3 = partitions3.partition(2);

    assertEquals(nestedPartitions1.members(), nestedPartitions2.members());
    assertEquals(nestedPartitions2.members(), nestedPartitions3.members());

    ConsistentHashGroup hash1 = nestedPartitions1.hash();
    ConsistentHashGroup hash2 = nestedPartitions2.hash();
    ConsistentHashGroup hash3 = nestedPartitions3.hash();

    assertEquals(hash1.members(), hash2.members());
    assertEquals(hash2.members(), hash3.members());
  }

  /**
   * Tests direct member tasks.
   */
  public void testDirectTask() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6000)));
    DistributedGroup group2 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6001)));

    LocalGroupMember member = group2.join().get(10, TimeUnit.SECONDS);

    assertEquals(group1.members().size(), 1);
    assertEquals(group2.members().size(), 1);

    member.tasks().onTask(task -> {
      threadAssertEquals(task.value(), "Hello world!");
      task.ack();
      resume();
    });
    group1.member(member.id()).tasks().submit("Hello world!").thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests failing a direct task.
   */
  public void testDirectTaskFail() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6000)));
    DistributedGroup group2 = createResource(new DistributedGroup.Options().withAddress(new Address("localhost", 6001)));

    LocalGroupMember member = group2.join().get(10, TimeUnit.SECONDS);

    assertEquals(group1.members().size(), 1);
    assertEquals(group2.members().size(), 1);

    member.tasks().onTask(task -> {
      threadAssertEquals(task.value(), "Hello world!");
      task.fail();
      resume();
    });
    group1.member(member.id()).tasks().submit("Hello world!").whenComplete((result, error) -> {
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

    LocalGroupMember member = group2.join().get(10, TimeUnit.SECONDS);

    assertEquals(group1.members().size(), 1);
    assertEquals(group2.members().size(), 1);

    member.tasks().onTask(task -> {
      member.leave().thenRun(this::resume);
      resume();
    });

    group1.member(member.id()).tasks().submit("Hello world!").whenComplete((result, error) -> {
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

    LocalGroupMember member1 = group1.join().get(10, TimeUnit.SECONDS);
    LocalGroupMember member2 = group2.join().get(10, TimeUnit.SECONDS);
    LocalGroupMember member3 = group2.join().get(10, TimeUnit.SECONDS);

    assertEquals(group1.members().size(), 3);
    assertEquals(group2.members().size(), 3);

    member1.tasks().onTask(task -> {
      threadAssertEquals(task.value(), "Hello world!");
      task.ack();
      resume();
    });
    member2.tasks().onTask(task -> {
      threadAssertEquals(task.value(), "Hello world!");
      task.ack();
      resume();
    });
    member3.tasks().onTask(task -> {
      threadAssertEquals(task.value(), "Hello world!");
      task.ack();
      resume();
    });
    group1.tasks().submit("Hello world!").thenRun(this::resume);
    await(10000, 4);
  }

}
