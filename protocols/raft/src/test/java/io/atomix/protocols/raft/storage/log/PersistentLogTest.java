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
package io.atomix.protocols.raft.storage.log;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Disk log test.
 */
public abstract class PersistentLogTest extends AbstractLogTest {
  protected PersistentLogTest(int maxSegmentSize, int cacheSize) {
    super(maxSegmentSize, cacheSize);
  }

  /**
   * Tests reading from a compacted log.
   */
  @Test
  public void testCompactAndRecover() throws Exception {
    RaftLog log = createLog();

    // Write three segments to the log.
    RaftLogWriter writer = log.writer();
    for (int i = 0; i < entriesPerSegment * 3; i++) {
      writer.append(ENTRY);
    }

    // Commit the entries and compact the first segment.
    writer.commit(entriesPerSegment * 3);
    log.compact(entriesPerSegment + 1);

    // Close the log.
    log.close();

    // Reopen the log and create a reader.
    log = createLog();
    writer = log.writer();
    RaftLogReader reader = log.openReader(1, RaftLogReader.Mode.COMMITS);
    writer.append(ENTRY);
    writer.append(ENTRY);
    writer.commit(entriesPerSegment * 3);

    // Ensure the reader starts at the first physical index in the log.
    assertEquals(entriesPerSegment + 1, reader.getNextIndex());
    assertEquals(reader.getFirstIndex(), reader.getNextIndex());
    assertTrue(reader.hasNext());
    assertEquals(entriesPerSegment + 1, reader.getNextIndex());
    assertEquals(reader.getFirstIndex(), reader.getNextIndex());
    assertEquals(entriesPerSegment + 1, reader.next().index());
  }
}
