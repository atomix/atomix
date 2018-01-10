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
package io.atomix.utils.time;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Wall clock test.
 */
public class WallClockTest {
  @Test
  public void testWallClock() throws Exception {
    WallClock clock = new WallClock();
    WallClockTimestamp time = clock.getTime();
    assertNotNull(time);
    Thread.sleep(5);
    assertTrue(clock.getTime().unixTimestamp() > time.unixTimestamp());
  }
}
