/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.time;

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
