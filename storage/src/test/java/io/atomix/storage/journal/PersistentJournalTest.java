// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Persistent journal test base.
 */
public abstract class PersistentJournalTest extends AbstractJournalTest {
  protected PersistentJournalTest(int maxSegmentSize, int cacheSize) {
    super(maxSegmentSize, cacheSize);
  }

  /**
   * Tests reading from a compacted journal.
   */
  @Test
  public void testCompactAndRecover() throws Exception {
    SegmentedJournal<TestEntry> journal = createJournal();

    // Write three segments to the journal.
    JournalWriter<TestEntry> writer = journal.writer();
    for (int i = 0; i < entriesPerSegment * 3; i++) {
      writer.append(ENTRY);
    }

    // Commit the entries and compact the first segment.
    writer.commit(entriesPerSegment * 3);
    journal.compact(entriesPerSegment + 1);

    // Close the journal.
    journal.close();

    // Reopen the journal and create a reader.
    journal = createJournal();
    writer = journal.writer();
    JournalReader<TestEntry> reader = journal.openReader(1, JournalReader.Mode.COMMITS);
    writer.append(ENTRY);
    writer.append(ENTRY);
    writer.commit(entriesPerSegment * 3);

    // Ensure the reader starts at the first physical index in the journal.
    assertEquals(entriesPerSegment + 1, reader.getNextIndex());
    assertEquals(reader.getFirstIndex(), reader.getNextIndex());
    assertTrue(reader.hasNext());
    assertEquals(entriesPerSegment + 1, reader.getNextIndex());
    assertEquals(reader.getFirstIndex(), reader.getNextIndex());
    assertEquals(entriesPerSegment + 1, reader.next().index());
  }
}
