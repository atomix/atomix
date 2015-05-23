/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft.storage;

import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.raft.storage.*;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class BufferedStorageTest {

  /**
   * Creates a log with a default configuration.
   */
  private BufferedStorage createLog() {
    return createLog(new LogConfig().withName(UUID.randomUUID().toString()));
  }

  public static class TestEntry extends RaftEntry<TestEntry> {
    private long value;

    public TestEntry(ReferenceManager<RaftEntry<?>> referenceManager) {
      super(referenceManager);
    }

    public TestEntry setValue(long value) {
      this.value = value;
      return this;
    }

    public long getValue() {
      return value;
    }
  }

  /**
   * Creates a log with a custom configuration.
   */
  private BufferedStorage createLog(LogConfig config) {
    BufferedStorage log = new BufferedStorage(config);
    log.open();
    return log;
  }

  /**
   * Performs assertions on an empty initialized log.
   */
  private void checkInitial(BufferedStorage log) {
    assertTrue(log.isOpen());
    assertTrue(log.isEmpty());
    assertEquals(log.firstIndex(), 0);
    assertEquals(log.lastIndex(), 0);
    assertEquals(log.length(), 0);
  }

  /**
   * Writes a keyed test entry.
   */
  private void writeTestEntry(BufferedStorage log) {
    try (TestEntry writer = log.createEntry(TestEntry.class)) {
      writer.setTerm(1);
      writer.setValue(1234);
    }
  }

  /**
   * Reads a keyed test entry.
   */
  private void readUncommittedKeyedTestEntry(BufferedStorage log, long index) {
    try (TestEntry reader = log.getEntry(index)) {
      assertEquals(reader.getIndex(), index);
      assertEquals(reader.getTerm(), 1);
      assertEquals(reader.getValue(), 1234);
    }
  }

  /**
   * Reads a keyed test entry.
   */
  private void readCommittedKeyedTestEntry(BufferedStorage log, long index) {
    assertTrue(log.containsIndex(index));
    assertTrue(log.commitIndex() >= index);
    try (TestEntry reader = log.getEntry(index)) {
      assertEquals(reader.getIndex(), index);
      assertEquals(reader.getTerm(), 1);
      assertEquals(reader.getValue(), 1234);
    }
  }

  /**
   * Reads a keyed entry that has been deduplicated.
   */
  private void readDeduplicatedTestEntry(BufferedStorage log, long index) {
    assertTrue(log.containsIndex(index));
    assertFalse(log.containsEntry(index));
    try (TestEntry reader = log.getEntry(index)) {
      assertNull(reader);
    }
  }

  /**
   * Checks the length of the log.
   */
  private void checkLength(BufferedStorage log, long length) {
    for (long i = 1; i <= length; i++)
      assertTrue(log.containsIndex(i));
    assertFalse(log.isEmpty());
    assertEquals(log.length(), length);
    assertEquals(log.firstIndex(), length >= 1 ? 1 : 0);
    assertEquals(log.lastIndex(), length);
  }

  /**
   * Tests that the log properly indicates whether it contains an index.
   */
  public void testContainsIndex() {
    BufferedStorage openLog;
    try (BufferedStorage log = createLog()) {
      openLog = log;
      assertFalse(log.containsIndex(0));
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests writing to and reading from the log.
   */
  public void testWriteReadKeyedEntry() {
    BufferedStorage openLog;
    try (BufferedStorage log = createLog()) {
      openLog = log;
      checkInitial(log);
      writeTestEntry(log);
      checkLength(log, 1);
      readUncommittedKeyedTestEntry(log, 1);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests reading an old entry from the log.
   */
  public void testReadOldEntry() {
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(new LogConfig().withName(UUID.randomUUID().toString()).withMaxEntriesPerSegment(8))) {
      openLog = log;
      checkInitial(log);
      for (int i = 0; i < 12; i++) {
        writeTestEntry(log);
      }
      readUncommittedKeyedTestEntry(log, 4);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests committing an entry.
   */
  public void testCommitEntry() {
    BufferedStorage openLog;
    try (BufferedStorage log = createLog()) {
      openLog = log;
      checkInitial(log);
      writeTestEntry(log);
      checkLength(log, 1);
      log.commit(1);
      assertEquals(log.commitIndex(), 1);
      readCommittedKeyedTestEntry(log, 1);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests committing entries across multiple segments.
   */
  public void testCommitEntryAfterSegment() {
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(new LogConfig().withName(UUID.randomUUID().toString()).withMaxEntriesPerSegment(128))) {
      openLog = log;
      checkInitial(log);
      for (int i = 0; i < 150; i++) {
        writeTestEntry(log);
      }
      checkLength(log, 150);
      log.commit(140);
      assertEquals(log.commitIndex(), 140);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests that entries are not deduplicated before commitment.
   */
  public void testNoDedupeBeforeCommit() {
    BufferedStorage openLog;
    try (BufferedStorage log = createLog()) {
      openLog = log;
      checkInitial(log);
      writeTestEntry(log);
      checkLength(log, 1);
      writeTestEntry(log);
      checkLength(log, 2);
      readUncommittedKeyedTestEntry(log, 1);
      readUncommittedKeyedTestEntry(log, 2);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests deleting a log.
   */
  public void testDelete() {
    LogConfig config = new LogConfig()
      .withName("test-delete");
    BufferedStorage log = createLog(config);
    SegmentManager manager = new SegmentManager(config);
    assertFalse(manager.loadSegments().isEmpty());
    log.close();
    log.delete();
    assertTrue(manager.loadSegments().isEmpty());
  }

  /**
   * Tests compacting the log.
   */
  public void testCompact() {
    LogConfig config = new LogConfig()
      .withName(UUID.randomUUID().toString())
      .withCompactInterval(Long.MAX_VALUE)
      .withMaxEntriesPerSegment(8);
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(config)) {
      openLog = log;
      for (int i = 0; i < 16; i++) {
        writeTestEntry(log);
      }
      checkLength(log, 16);
      log.commit(12);
      assertEquals(log.commitIndex(), 12);
      log.compactNow();
      readCommittedKeyedTestEntry(log, 8);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests reading an entry from the log after recovering.
   */
  public void testRecoverRead() {
    LogConfig config = new LogConfig()
      .withName(UUID.randomUUID().toString())
      .withCompactInterval(Long.MAX_VALUE)
      .withMaxEntriesPerSegment(8);
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(config)) {
      writeTestEntry(log);
    }
    try (BufferedStorage log = createLog(config)) {
      openLog = log;
      readUncommittedKeyedTestEntry(log, 1);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests that the log is properly recovered after shutdown.
   */
  public void testRecoverBeforeCompact() {
    LogConfig config = new LogConfig()
      .withName(UUID.randomUUID().toString())
      .withCompactInterval(Long.MAX_VALUE)
      .withMaxEntriesPerSegment(8);
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(config)) {
      for (int i = 0; i < 14; i++) {
        writeTestEntry(log);
      }
      readUncommittedKeyedTestEntry(log, 1);
      checkLength(log, 14);
    }
    try (BufferedStorage log = createLog(config)) {
      openLog = log;
      assertEquals(log.commitIndex(), 0);
      readUncommittedKeyedTestEntry(log, 1);
      log.commit(12);
      assertEquals(log.commitIndex(), 12);
      readCommittedKeyedTestEntry(log, 8);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests that the log is properly recovered after compaction and shutdown.
   */
  public void testRecoverAfterCompact() {
    LogConfig config = new LogConfig()
      .withName(UUID.randomUUID().toString())
      .withCompactInterval(Long.MAX_VALUE)
      .withMaxEntriesPerSegment(8);
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(config)) {
      for (int i = 0; i < 14; i++) {
        writeTestEntry(log);
      }
      checkLength(log, 14);
      log.commit(12);
      assertEquals(log.commitIndex(), 12);
    }
    try (BufferedStorage log = createLog(config)) {
      openLog = log;
      assertEquals(log.commitIndex(), 0);
      log.commit(12);
      assertEquals(log.commitIndex(), 12);
      readCommittedKeyedTestEntry(log, 8);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests that a segment descriptor is persisted across log instances.
   */
  public void testPersistSegmentDescriptor() {
    LogConfig config = new LogConfig()
      .withName("test")
      .withMaxEntriesPerSegment(1024)
      .withCompactInterval(10, TimeUnit.SECONDS);

    try (BufferedStorage log = new BufferedStorage(config)) {
      log.open();
      Segment segment = log.segments.currentSegment();
      assertEquals(segment.descriptor().id(), 1);
      assertEquals(segment.descriptor().version(), 1);
      assertEquals(segment.descriptor().index(), 1);
      assertEquals(segment.descriptor().maxSegmentSize(), 1024);
    }

    try (BufferedStorage log = new BufferedStorage(config)) {
      log.open();
      Segment segment = log.segments.currentSegment();
      assertEquals(segment.descriptor().id(), 1);
      assertEquals(segment.descriptor().version(), 1);
      assertEquals(segment.descriptor().index(), 1);
      assertEquals(segment.descriptor().maxSegmentSize(), 1024);
      log.delete();
    }
  }

}
