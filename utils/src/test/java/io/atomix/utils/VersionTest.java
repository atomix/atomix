// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Version test.
 */
public class VersionTest {
  @Test
  public void testVersionComparison() {
    assertTrue(Version.from("1.0.0").compareTo(Version.from("2.0.0")) < 0);
    assertTrue(Version.from("2.0.0").compareTo(Version.from("1.0.0")) > 0);
    assertTrue(Version.from("1.0.0").compareTo(Version.from("0.1.0")) > 0);
    assertTrue(Version.from("0.1.0").compareTo(Version.from("1.0.0")) < 0);
    assertTrue(Version.from("0.1.0").compareTo(Version.from("0.1.1")) < 0);
    assertTrue(Version.from("1.0.0").compareTo(Version.from("0.0.1")) > 0);
    assertTrue(Version.from("1.1.1").compareTo(Version.from("1.0.3")) > 0);
    assertTrue(Version.from("1.0.0").compareTo(Version.from("1.0.0-beta1")) > 0);
    assertTrue(Version.from("1.0.0-rc2").compareTo(Version.from("1.0.0-rc1")) > 0);
    assertTrue(Version.from("1.0.0-rc1").compareTo(Version.from("1.0.0-beta1")) > 0);
    assertTrue(Version.from("2.0.0-beta1").compareTo(Version.from("1.0.0")) > 0);
    assertTrue(Version.from("1.0.0-alpha1").compareTo(Version.from("1.0.0-SNAPSHOT")) > 0);
  }

  @Test
  public void testVersionToString() {
    assertEquals("1.0.0", Version.from("1.0.0").toString());
    assertEquals("1.0.0-alpha1", Version.from("1.0.0-alpha1").toString());
    assertEquals("1.0.0-beta1", Version.from("1.0.0-beta1").toString());
    assertEquals("1.0.0-rc1", Version.from("1.0.0-rc1").toString());
    assertEquals("1.0.0-SNAPSHOT", Version.from("1.0.0-SNAPSHOT").toString());
  }

  @Test
  public void testInvalidVersions() {
    assertIllegalArgument(() -> Version.from("1"));
    assertIllegalArgument(() -> Version.from("1.0"));
    assertIllegalArgument(() -> Version.from("1.0-beta1"));
    assertIllegalArgument(() -> Version.from("1.0.0.0"));
    assertIllegalArgument(() -> Version.from("1.0.0.0-beta1"));
    assertIllegalArgument(() -> Version.from("1.0.0-not1"));
    assertIllegalArgument(() -> Version.from("1.0.0-alpha"));
    assertIllegalArgument(() -> Version.from("1.0.0-beta"));
    assertIllegalArgument(() -> Version.from("1.0.0-rc"));
  }

  private void assertIllegalArgument(Runnable callback) {
    try {
      callback.run();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }
}
