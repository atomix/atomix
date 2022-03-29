// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

import com.google.common.testing.EqualsTester;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * MultiValuedTimestamp unit tests.
 */
public class MultiValuedTimestampTest {
  private final MultiValuedTimestamp<Integer, Integer> stats1 = new MultiValuedTimestamp<>(1, 3);
  private final MultiValuedTimestamp<Integer, Integer> stats2 = new MultiValuedTimestamp<>(1, 2);

  /**
   * Tests the creation of the MapEvent object.
   */
  @Test
  public void testConstruction() {
    assertThat(stats1.value1(), is(1));
    assertThat(stats1.value2(), is(3));
  }

  /**
   * Tests the toCompare function.
   */
  @Test
  public void testToCompare() {
    assertThat(stats1.compareTo(stats2), is(1));
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

  /**
   * Tests that the empty argument list constructor for serialization
   * is present and creates a proper object.
   */
  @Test
  public void testSerializerConstructor() {
    try {
      Constructor[] constructors = MultiValuedTimestamp.class.getDeclaredConstructors();
      assertThat(constructors, notNullValue());
      Arrays.stream(constructors).filter(ctor ->
          ctor.getParameterTypes().length == 0)
          .forEach(noParamsCtor -> {
            try {
              noParamsCtor.setAccessible(true);
              MultiValuedTimestamp stats =
                  (MultiValuedTimestamp) noParamsCtor.newInstance();
              assertThat(stats, notNullValue());
            } catch (Exception e) {
              Assert.fail("Exception instantiating no parameters constructor");
            }
          });
    } catch (Exception e) {
      Assert.fail("Exception looking up constructors");
    }
  }
}
