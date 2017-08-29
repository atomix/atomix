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
package io.atomix.protocols.raft.storage.snapshot;

import org.junit.Test;

import java.io.File;
import java.util.Date;

import static org.junit.Assert.assertEquals;
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
    long timestamp = 3;
    String timestampString = SnapshotFile.TIMESTAMP_FORMAT.format(new Date(timestamp));
    String fileName = SnapshotFile.createSnapshotFileName("test", 1, 2, timestamp);
    assertEquals(fileName, "test-1-2-" + timestampString + ".snapshot");
  }

  /**
   * Tests determining whether a file is a snapshot file.
   */
  @Test
  public void testCreateValidateSnapshotFile() throws Exception {
    File file = SnapshotFile.createSnapshotFile("test", new File(System.getProperty("user.dir")), 1, 2, 3);
    assertTrue(SnapshotFile.isSnapshotFile("test", file));
  }

  /**
   * Tests parsing the snapshot identifier.
   */
  @Test
  public void testParseSnapshotId() throws Exception {
    String fileName = SnapshotFile.createSnapshotFileName("test", 1, 2, 3);
    assertEquals(SnapshotFile.parseId(fileName), 1);
  }

  /**
   * Tests parsing the snapshot index.
   */
  @Test
  public void testParseSnapshotIndex() throws Exception {
    String fileName = SnapshotFile.createSnapshotFileName("test", 1, 2, 3);
    assertEquals(SnapshotFile.parseIndex(fileName), 2);
  }

  /**
   * Tests parsing the snapshot timestamp.
   */
  @Test
  public void testParseSnapshotTimestamp() throws Exception {
    String fileName = SnapshotFile.createSnapshotFileName("test", 1, 2, 3);
    assertEquals(SnapshotFile.parseTimestamp(fileName), SnapshotFile.TIMESTAMP_FORMAT.parse(SnapshotFile.TIMESTAMP_FORMAT.format(3)).getTime());
  }

  /**
   * Tests parsing a timestamp string.
   */
  @Test
  public void testTimestampDecoder() throws Exception {
    String timestampString = "20170624151018000";
    SnapshotFile.TIMESTAMP_FORMAT.parse(timestampString);
  }

}
