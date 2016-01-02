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

import io.atomix.resource.ResourceType;
import io.atomix.testing.AbstractCopycatTest;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Async group test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedMembershipGroupTest extends AbstractCopycatTest<DistributedMembershipGroup> {
  
  @Override
  protected ResourceType<DistributedMembershipGroup> type() {
    return DistributedMembershipGroup.TYPE;
  }

  /**
   * Tests joining a group.
   */
  public void testJoin() throws Throwable {
    createServers(3);

    DistributedMembershipGroup group1 = createResource();
    DistributedMembershipGroup group2 = createResource();

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

    DistributedMembershipGroup group1 = createResource();
    DistributedMembershipGroup group2 = createResource();

    group2.join().thenRun(() -> {
      group2.members().thenAccept(members -> {
        threadAssertEquals(members.size(), 1);
        resume();
      });
    });

    await(5000);

    group1.join().thenRun(() -> {
      group1.members().thenAccept(members -> {
        threadAssertEquals(members.size(), 2);
        resume();
      });
      group2.members().thenAccept(members -> {
        threadAssertEquals(members.size(), 2);
        resume();
      });
    });

    await(10000, 2);

    group1.onLeave(member -> resume());
    group2.leave().thenRun(this::resume);

    await(10000, 2);
  }

  /**
   * Tests setting and getting member properties.
   */
  public void testProperties() throws Throwable {
    createServers(3);

    DistributedMembershipGroup group1 = createResource();
    DistributedMembershipGroup group2 = createResource();

    group1.join().thenAccept(member -> {
      member.set("foo", "Hello world!").thenRun(this::resume);
    });

    await(5000);

    group2.members().thenAccept(members -> {
      threadAssertEquals(members.size(), 1);
      members.iterator().next().get("foo").thenAccept(result -> {
        threadAssertEquals(result, "Hello world!");
        resume();
      });
    });

    await(5000);

    group1.member().remove("foo").thenRun(this::resume);
    await(5000);

    group2.members().thenAccept(members -> {
      threadAssertEquals(members.size(), 1);
      members.iterator().next().get("foo").thenAccept(result -> {
        threadAssertNull(result);
        resume();
      });
    });

    await(5000);
  }

  /**
   * Tests sending message between members.
   */
  public void testSend() throws Throwable {
    createServers(3);

    DistributedMembershipGroup group1 = createResource();
    DistributedMembershipGroup group2 = createResource();

    group1.join().thenAccept(member -> {
      member.onMessage("foo", message -> {
        threadAssertEquals(message, "Hello world!");
        resume();
      });
      resume();
    });

    await(5000);

    group2.members().thenAccept(members -> {
      threadAssertEquals(members.size(), 1);
      members.iterator().next().send("foo", "Hello world!").thenRun(this::resume);
    });

    await(10000, 2);
  }

  /**
   * Tests executing an immediate callback.
   */
  public void testRemoteExecute() throws Throwable {
    createServers(3);

    DistributedMembershipGroup group1 = createResource();
    DistributedMembershipGroup group2 = createResource();

    group2.join().thenRun(() -> {
      group2.members().thenAccept(members -> {
        threadAssertEquals(members.size(), 1);
        resume();
      });
    });

    await(5000);

    AtomicInteger counter = new AtomicInteger();
    group1.join().thenRun(() -> {
      group1.members().thenAccept(members -> {
        for (GroupMember member : members) {
          member.execute((Runnable & Serializable) counter::incrementAndGet).thenRun(this::resume);
        }
      });
    });

    await(10000, 2);
  }

}
