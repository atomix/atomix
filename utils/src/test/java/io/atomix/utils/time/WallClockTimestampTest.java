// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link WallClockTimestamp}.
 */
public class WallClockTimestampTest {
  @Test
  public final void testBasic() throws InterruptedException {
    WallClockTimestamp ts1 = new WallClockTimestamp();
    Thread.sleep(50);
    WallClockTimestamp ts2 = new WallClockTimestamp();
    long stamp = System.currentTimeMillis() + 10000;
    WallClockTimestamp ts3 = new WallClockTimestamp(stamp);

    assertTrue(ts1.compareTo(ts1) == 0);
    assertTrue(ts2.compareTo(ts1) > 0);
    assertTrue(ts1.compareTo(ts2) < 0);
    assertTrue(ts3.unixTimestamp() == stamp);
  }
}
