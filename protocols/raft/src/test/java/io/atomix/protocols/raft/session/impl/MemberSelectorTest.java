/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.raft.session.impl;

import io.atomix.cluster.MemberId;
import io.atomix.protocols.raft.session.CommunicationStrategy;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Member selector test.
 */
public class MemberSelectorTest {

  /**
   * Tests selecting members using the ANY selector.
   */
  @Test
  public void testSelectAny() throws Exception {
    MemberSelectorManager selectorManager = new MemberSelectorManager();
    MemberSelector selector = selectorManager.createSelector(CommunicationStrategy.ANY);

    assertNull(selector.leader());
    assertFalse(selector.hasNext());

    selectorManager.resetAll(null, Arrays.asList(MemberId.from("a"), MemberId.from("b"), MemberId.from("c")));
    assertNull(selector.leader());
    assertTrue(selector.hasNext());
    selector.hasNext();
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertFalse(selector.hasNext());
    selector.reset();
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertFalse(selector.hasNext());

    selectorManager.resetAll(MemberId.from("a"), Arrays.asList(MemberId.from("a"), MemberId.from("b"), MemberId.from("c")));
    assertNotNull(selector.leader());
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertFalse(selector.hasNext());
    selector.reset();
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertFalse(selector.hasNext());
  }

  /**
   * Tests selecting members using the FOLLOWER selector.
   */
  @Test
  public void testSelectFollower() throws Exception {
    MemberSelectorManager selectorManager = new MemberSelectorManager();
    MemberSelector selector = selectorManager.createSelector(CommunicationStrategy.FOLLOWERS);

    assertNull(selector.leader());
    assertFalse(selector.hasNext());

    selectorManager.resetAll(null, Arrays.asList(MemberId.from("a"), MemberId.from("b"), MemberId.from("c")));
    assertNull(selector.leader());
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertFalse(selector.hasNext());
    selector.reset();
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertFalse(selector.hasNext());

    selectorManager.resetAll(MemberId.from("a"), Arrays.asList(MemberId.from("a"), MemberId.from("b"), MemberId.from("c")));
    assertNotNull(selector.leader());
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertFalse(selector.hasNext());
  }

  /**
   * Tests the member selector.
   */
  @Test
  public void testSelectLeader() throws Exception {
    MemberSelectorManager selectorManager = new MemberSelectorManager();
    MemberSelector selector = selectorManager.createSelector(CommunicationStrategy.LEADER);

    assertNull(selector.leader());
    assertFalse(selector.hasNext());

    selectorManager.resetAll(null, Arrays.asList(MemberId.from("a"), MemberId.from("b"), MemberId.from("c")));
    assertNull(selector.leader());
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertFalse(selector.hasNext());
    selector.reset();
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertNotNull(selector.next());
    assertFalse(selector.hasNext());

    selectorManager.resetAll(MemberId.from("a"), Arrays.asList(MemberId.from("a"), MemberId.from("b"), MemberId.from("c")));
    assertEquals(MemberId.from("a"), selector.leader());
    assertEquals(3, selector.members().size());
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertFalse(selector.hasNext());

    selectorManager.resetAll(null, Arrays.asList(MemberId.from("a"), MemberId.from("b"), MemberId.from("c")));
    assertNull(selector.leader());
    assertTrue(selector.hasNext());

    selectorManager.resetAll(MemberId.from("a"), Arrays.asList(MemberId.from("b"), MemberId.from("c")));
    assertNull(selector.leader());
    assertTrue(selector.hasNext());
  }

}
