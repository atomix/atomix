// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

import com.google.common.base.Objects;
import io.atomix.utils.misc.Match;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * Unit tests for Match.
 */
public class MatchTest {

  @Test
  public void testMatches() {
    Match<String> m1 = Match.any();
    assertTrue(m1.matches(null));
    assertTrue(m1.matches("foo"));
    assertTrue(m1.matches("bar"));

    Match<String> m2 = Match.ifNull();
    assertTrue(m2.matches(null));
    assertFalse(m2.matches("foo"));

    Match<String> m3 = Match.ifValue("foo");
    assertFalse(m3.matches(null));
    assertFalse(m3.matches("bar"));
    assertTrue(m3.matches("foo"));

    Match<byte[]> m4 = Match.ifValue(new byte[8]);
    assertTrue(m4.matches(new byte[8]));
    assertFalse(m4.matches(new byte[7]));
  }

  @Test
  public void testEquals() {
    Match<String> m1 = Match.any();
    Match<String> m2 = Match.any();
    Match<String> m3 = Match.ifNull();
    Match<String> m4 = Match.ifValue("bar");
    assertEquals(m1, m2);
    assertFalse(Objects.equal(m1, m3));
    assertFalse(Objects.equal(m3, m4));
    Object o = new Object();
    assertFalse(Objects.equal(m1, o));
  }

  @Test
  public void testMap() {
    Match<String> m1 = Match.ifNull();
    assertEquals(m1.map(s -> "bar"), Match.ifNull());
    Match<String> m2 = Match.ifValue("foo");
    Match<String> m3 = m2.map(s -> "bar");
    assertTrue(m3.matches("bar"));
  }

  @Test
  public void testIfNotNull() {
    Match<String> m = Match.ifNotNull();
    assertFalse(m.matches(null));
    assertTrue(m.matches("foo"));
  }

  @Test
  public void testIfNotValue() {
    Match<String> m1 = Match.ifNotValue(null);
    Match<String> m2 = Match.ifNotValue("foo");
    assertFalse(m1.matches(null));
    assertFalse(m2.matches("foo"));
  }

  @Test
  public void testToString() {
    Match<String> m1 = Match.any();
    Match<String> m2 = Match.any();
    Match<String> m3 = Match.ifValue("foo");
    Match<String> m4 = Match.ifValue("foo");
    Match<String> m5 = Match.ifNotValue("foo");

    String note = "Results of toString() should be consistent -- ";

    assertTrue(note, m1.toString().equals(m2.toString()));
    assertTrue(note, m3.toString().equals(m4.toString()));
    assertFalse(note, m4.toString().equals(m5.toString()));
  }

  @Test
  public void testHashCode() {
    Match<String> m1 = Match.ifValue("foo");
    Match<String> m2 = Match.ifNotValue("foo");
    Match<String> m3 = Match.ifValue("foo");
    Match<String> m4 = Match.ifNotNull();
    Match<String> m5 = Match.ifNull();

    assertTrue(m1.hashCode() == m3.hashCode());
    assertFalse(m2.hashCode() == m1.hashCode());
    assertFalse(m4.hashCode() == m5.hashCode());

  }

}
