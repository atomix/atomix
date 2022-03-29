// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Logical clock test.
 */
public class LogicalClockTest {
  @Test
  public void testLogicalClock() throws Exception {
    LogicalClock clock = new LogicalClock();
    assertEquals(1, clock.increment().value());
    assertEquals(1, clock.getTime().value());
    assertEquals(2, clock.increment().value());
    assertEquals(2, clock.getTime().value());
    assertEquals(5, clock.update(LogicalTimestamp.of(5)).value());
    assertEquals(5, clock.getTime().value());
    assertEquals(5, clock.update(LogicalTimestamp.of(3)).value());
  }
}
