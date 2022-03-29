// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.session.impl;

import io.atomix.cluster.MemberId;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Member selector manager test.
 */
public class MemberSelectorManagerTest {

  /**
   * Tests the member selector manager.
   */
  @Test
  public void testMemberSelectorManager() throws Exception {
    MemberSelectorManager selectorManager = new MemberSelectorManager();
    assertNull(selectorManager.leader());
    assertEquals(0, selectorManager.members().size());
    selectorManager.resetAll();
    assertNull(selectorManager.leader());
    assertEquals(0, selectorManager.members().size());
    selectorManager.resetAll(MemberId.from("a"), Arrays.asList(MemberId.from("a"), MemberId.from("b"), MemberId.from("c")));
    assertEquals(MemberId.from("a"), selectorManager.leader());
    assertEquals(3, selectorManager.members().size());
  }

}
