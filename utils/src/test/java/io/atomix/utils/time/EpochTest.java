// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Logical timestamp test.
 */
public class EpochTest {
  @Test
  public void testLogicalTimestamp() throws Exception {
    Epoch epoch = Epoch.of(1);
    assertEquals(1, epoch.value());
    assertTrue(epoch.isNewerThan(Epoch.of(0)));
    assertFalse(epoch.isNewerThan(Epoch.of(2)));
    assertTrue(epoch.isOlderThan(Epoch.of(2)));
    assertFalse(epoch.isOlderThan(Epoch.of(0)));
  }
}
