// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.time;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Version test.
 */
public class VersionTest {
  @Test
  public void testVersion() {
    Version version1 = new Version(1);
    Version version2 = new Version(1);
    assertTrue(version1.equals(version2));
    assertTrue(version1.hashCode() == version2.hashCode());
    assertTrue(version1.value() == version2.value());

    Version version3 = new Version(2);
    assertFalse(version1.equals(version3));
    assertFalse(version1.hashCode() == version3.hashCode());
    assertFalse(version1.value() == version3.value());
  }
}
