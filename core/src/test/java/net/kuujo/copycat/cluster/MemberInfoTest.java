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

import net.kuujo.copycat.cluster.internal.MemberInfo;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Cluster member info test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class MemberInfoTest {

  /**
   * Tests updating a member state.
   */
  public void testUpdateState() {
    MemberInfo member = new MemberInfo("test", Member.Type.PASSIVE, Member.State.ALIVE, 5);
    member.update(new MemberInfo("test", Member.Type.PASSIVE, Member.State.SUSPICIOUS, 6));
    assertEquals(member.state(), Member.State.SUSPICIOUS);
  }

  /**
   * Tests rejecting an old state.
   */
  public void testRejectOldState() {
    MemberInfo member = new MemberInfo("test", Member.Type.PASSIVE, Member.State.ALIVE, 5);
    member.update(new MemberInfo("test", Member.Type.PASSIVE, Member.State.SUSPICIOUS, 4));
    assertEquals(member.state(), Member.State.ALIVE);
  }

  /**
   * Tests failing a member.
   */
  public void testFailures() {
    MemberInfo member = new MemberInfo("test", Member.Type.PASSIVE, Member.State.ALIVE, 5);
    member.fail("foo");
    assertEquals(member.state(), Member.State.SUSPICIOUS);
    member.fail("bar");
    assertEquals(member.state(), Member.State.SUSPICIOUS);
    member.fail("bar");
    assertEquals(member.state(), Member.State.SUSPICIOUS);
    member.fail("baz");
    assertEquals(member.state(), Member.State.DEAD);
  }

  /**
   * Tests that failures are reset when the state is updated.
   */
  public void testResetFailuresOnStateUpdate() {
    MemberInfo member = new MemberInfo("test", Member.Type.PASSIVE, Member.State.ALIVE, 5);
    member.fail("foo");
    member.fail("bar");
    assertEquals(member.failures().size(), 2);
    assertEquals(member.state(), Member.State.SUSPICIOUS);
    member.update(new MemberInfo("test", Member.Type.PASSIVE, Member.State.ALIVE, 6));
    assertEquals(member.state(), Member.State.ALIVE);
  }

}
