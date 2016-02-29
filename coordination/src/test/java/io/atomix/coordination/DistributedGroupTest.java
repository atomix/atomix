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
package io.atomix.coordination;

import static org.testng.Assert.assertEquals;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.annotations.Test;

import io.atomix.testing.AbstractCopycatTest;

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
  public void testExpire() throws Throwable {
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
    localMember.set("foo", "Hello world!").thenRun(this::resume);

    await(5000);

    assertEquals(group2.members().size(), 1);
    group2.members().iterator().next().get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(5000);

    localMember.remove("foo").thenRun(this::resume);
    await(5000);

    assertEquals(group2.members().size(), 1);
    group2.members().iterator().next().get("foo").thenAccept(result -> {
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

    LocalGroupMember localMember = group1.join(memberId).get();

    localMember.set("foo", "Hello world!").thenRun(this::resume);
    await(5000);

    assertEquals(group2.members().size(), 1);
    group2.members().iterator().next().get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });
    await(5000);

    group1.close().thenRun(this::resume);
    await(5000);

    LocalGroupMember localMember2 = group2.join(memberId).get();

    assertEquals(group2.members().size(), 1);
    localMember2.get("foo").thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(5000);
  }

  /**
   * Tests sending message between members.
   */
  public void testSend() throws Throwable {
    createServers(3);

    DistributedGroup group1 = createResource();
    DistributedGroup group2 = createResource();

    group1.join().thenAccept(member -> {
      member.onMessage("foo", message -> {
        threadAssertEquals(message, "Hello world!");
        resume();
      });
      resume();
    });

    await(5000);

    assertEquals(group2.members().size(), 1);
    group2.members().iterator().next().send("foo", "Hello world!").thenRun(this::resume);

    await(10000, 2);
  }

}
