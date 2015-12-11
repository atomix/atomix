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

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import io.atomix.atomix.testing.AbstractAtomixTest;
import io.atomix.coordination.state.MembershipGroupState;
import io.atomix.resource.ResourceStateMachine;

/**
 * Async group test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedMembershipGroupTest extends AbstractAtomixTest {

  @Override
  protected ResourceStateMachine createStateMachine() {
    return new MembershipGroupState();
  }

  /**
   * Tests joining a group.
   */
  public void testJoin() throws Throwable {
    createServers(3);

    DistributedMembershipGroup group1 = new DistributedMembershipGroup(createClient());
    DistributedMembershipGroup group2 = new DistributedMembershipGroup(createClient());

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

    DistributedMembershipGroup group1 = new DistributedMembershipGroup(createClient());
    DistributedMembershipGroup group2 = new DistributedMembershipGroup(createClient());

    group2.join().thenRun(() -> {
      threadAssertEquals(group2.members().size(), 1);
      resume();
    });

    await();

    group1.join().thenRun(() -> {
      threadAssertEquals(group1.members().size(), 2);
      threadAssertEquals(group2.members().size(), 2);
      group1.onLeave(member -> resume());
      group2.leave().thenRun(this::resume);
    });

    await(0, 2);
  }

  /**
   * Tests executing an immediate callback.
   */
  public void testRemoteExecute() throws Throwable {
    createServers(3);

    DistributedMembershipGroup group1 = new DistributedMembershipGroup(createClient());
    DistributedMembershipGroup group2 = new DistributedMembershipGroup(createClient());

    group2.join().thenRun(() -> {
      threadAssertEquals(group2.members().size(), 1);
      resume();
    });

    await();

    AtomicInteger counter = new AtomicInteger();
    group1.join().thenRun(() -> {
      for (GroupMember member : group1.members()) {
        member.execute((Runnable & Serializable) counter::incrementAndGet).thenRun(this::resume);
      }
    });

    await(0, 2);
  }

}
