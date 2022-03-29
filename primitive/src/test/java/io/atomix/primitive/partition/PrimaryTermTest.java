// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.cluster.MemberId;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Primary term test.
 */
public class PrimaryTermTest {
  @Test
  public void testEmptyTerm() throws Exception {
    PrimaryTerm term = new PrimaryTerm(1, null, Collections.emptyList());
    assertNull(term.primary());
    assertTrue(term.candidates().isEmpty());
    assertTrue(term.backups(0).isEmpty());
    assertTrue(term.backups(1).isEmpty());
    assertTrue(term.backups(2).isEmpty());
  }

  @Test
  public void testPrimaryTerm() throws Exception {
    GroupMember primary = new GroupMember(MemberId.from("1"), MemberGroupId.from("1"));
    List<GroupMember> candidates = Arrays.asList(
        new GroupMember(MemberId.from("1"), MemberGroupId.from("1")),
        new GroupMember(MemberId.from("2"), MemberGroupId.from("2")),
        new GroupMember(MemberId.from("3"), MemberGroupId.from("2")),
        new GroupMember(MemberId.from("4"), MemberGroupId.from("3")),
        new GroupMember(MemberId.from("5"), MemberGroupId.from("3")));

    PrimaryTerm term = new PrimaryTerm(1, primary, candidates);
    assertEquals(primary, term.primary());
    assertEquals(candidates, term.candidates());

    assertEquals(Arrays.asList(), term.backups(0));
    assertEquals(Arrays.asList(candidates.get(1)), term.backups(1));
    assertEquals(Arrays.asList(candidates.get(1), candidates.get(2)), term.backups(2));
    assertEquals(Arrays.asList(candidates.get(1), candidates.get(2), candidates.get(3)), term.backups(3));
    assertEquals(Arrays.asList(candidates.get(1), candidates.get(2), candidates.get(3), candidates.get(4)), term.backups(4));
    assertEquals(Arrays.asList(candidates.get(1), candidates.get(2), candidates.get(3), candidates.get(4)), term.backups(5));
  }
}
