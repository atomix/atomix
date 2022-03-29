// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Versioned unit tests.
 */
public class VersionedTest {

  private final Versioned<Integer> stats1 = new Versioned<>(1, 2, 3);

  private final Versioned<Integer> stats2 = new Versioned<>(1, 2);

  /**
   * Tests the creation of the MapEvent object.
   */
  @Test
  public void testConstruction() {
    assertThat(stats1.value(), is(1));
    assertThat(stats1.version(), is(2L));
    assertThat(stats1.creationTime(), is(3L));
  }

  /**
   * Maps an Integer to a String - Utility function to test the map function.
   *
   * @param a Actual Integer parameter.
   * @return String Mapped valued.
   */
  public static String transform(Integer a) {
    return Integer.toString(a);
  }

  /**
   * Tests the map function.
   */
  @Test
  public void testMap() {
    Versioned<String> tempObj = stats1.map(VersionedTest::transform);
    assertThat(tempObj.value(), is("1"));
  }

  /**
   * Tests the valueOrElse method.
   */
  @Test
  public void testOrElse() {
    Versioned<String> vv = new Versioned<>("foo", 1);
    Versioned<String> nullVV = null;
    assertThat(Versioned.valueOrElse(vv, "bar"), is("foo"));
    assertThat(Versioned.valueOrElse(nullVV, "bar"), is("bar"));
  }

  /**
   * Tests the equals, hashCode and toString methods using Guava EqualsTester.
   */
  @Test
  public void testEquals() {
    new EqualsTester()
        .addEqualityGroup(stats1, stats1)
        .addEqualityGroup(stats2)
        .testEquals();
  }

}
