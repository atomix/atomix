// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.snapshot;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Snapshot file test.
 */
public class SnapshotFileTest {

  /**
   * Tests creating a snapshot file name.
   */
  @Test
  public void testCreateSnapshotFileName() throws Exception {
    assertEquals("test-1.snapshot", SnapshotFile.createSnapshotFileName("test", 1));
    assertEquals("test-2.snapshot", SnapshotFile.createSnapshotFileName("test", 2));
  }

  /**
   * Tests determining whether a file is a snapshot file.
   */
  @Test
  public void testCreateValidateSnapshotFile() throws Exception {
    assertTrue(SnapshotFile.isSnapshotFile(SnapshotFile.createSnapshotFile(new File(System.getProperty("user.dir")), "foo", 1)));
    assertTrue(SnapshotFile.isSnapshotFile(SnapshotFile.createSnapshotFile(new File(System.getProperty("user.dir")), "foo-bar", 1)));
    assertFalse(SnapshotFile.isSnapshotFile(new File(System.getProperty("user.dir") + "/foo")));
    assertFalse(SnapshotFile.isSnapshotFile(new File(System.getProperty("user.dir") + "/foo.bar")));
    assertFalse(SnapshotFile.isSnapshotFile(new File(System.getProperty("user.dir") + "/foo.snapshot")));
    assertFalse(SnapshotFile.isSnapshotFile(new File(System.getProperty("user.dir") + "/foo-bar.snapshot")));
  }

  @Test
  public void testParseSnapshotName() throws Exception {
    assertEquals("foo", SnapshotFile.parseName("foo-1-2.snapshot"));
    assertEquals("foo-bar", SnapshotFile.parseName("foo-bar-1-2.snapshot"));
  }

}
