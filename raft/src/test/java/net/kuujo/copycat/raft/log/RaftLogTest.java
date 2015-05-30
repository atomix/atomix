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
package net.kuujo.copycat.raft.log;

import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.raft.log.entry.Entry;
import net.kuujo.copycat.util.ExecutionContext;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.*;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class RaftLogTest {

  /**
   * Creates a log with a default configuration.
   */
  private Log createLog() {
    return createLog(new LogConfig().withDirectory(UUID.randomUUID().toString()));
  }

  public static class TestEntry extends Entry<TestEntry> {
    private long value;

    public TestEntry(ReferenceManager<Entry<?>> referenceManager) {
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
  private Log createLog(LogConfig config) {
    Log log = new Log(new SegmentManager(config));
    log.open(new ExecutionContext("test", new Serializer()));
    return log;
  }

  /**
   * Performs assertions on an empty initialized log.
   */
  private void checkInitial(Log log) {
    assertTrue(log.isOpen());
    assertTrue(log.isEmpty());
    assertEquals(log.firstIndex(), 0);
    assertEquals(log.lastIndex(), 0);
    assertEquals(log.length(), 0);
  }

  /**
   * Writes a keyed test entry.
   */
  private void writeTestEntry(Log log) {
    try (TestEntry writer = log.createEntry(TestEntry.class)) {
      writer.setTerm(1);
      writer.setValue(1234);
    }
  }

  /**
   * Reads a keyed test entry.
   */
  private void readUncommittedKeyedTestEntry(Log log, long index) {
    try (TestEntry reader = log.getEntry(index)) {
      assertEquals(reader.getIndex(), index);
      assertEquals(reader.getTerm(), 1);
      assertEquals(reader.getValue(), 1234);
    }
  }

  /**
   * Reads a keyed test entry.
   */
  private void readCommittedKeyedTestEntry(Log log, long index) {
    assertTrue(log.containsIndex(index));
    try (TestEntry reader = log.getEntry(index)) {
      assertEquals(reader.getIndex(), index);
      assertEquals(reader.getTerm(), 1);
      assertEquals(reader.getValue(), 1234);
    }
  }

  /**
   * Reads a keyed entry that has been deduplicated.
   */
  private void readDeduplicatedTestEntry(Log log, long index) {
    assertTrue(log.containsIndex(index));
    assertFalse(log.containsEntry(index));
    try (TestEntry reader = log.getEntry(index)) {
      assertNull(reader);
    }
  }

  /**
   * Checks the length of the log.
   */
  private void checkLength(Log log, long length) {
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
    Log openLog;
    try (Log log = createLog()) {
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
    Log openLog;
    try (Log log = createLog()) {
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
    Log openLog;
    try (Log log = createLog(new LogConfig().withDirectory(UUID.randomUUID().toString()).withMaxEntriesPerSegment(8))) {
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
    Log openLog;
    try (Log log = createLog()) {
      openLog = log;
      checkInitial(log);
      writeTestEntry(log);
      checkLength(log, 1);
      readCommittedKeyedTestEntry(log, 1);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests committing entries across multiple segments.
   */
  public void testCommitEntryAfterSegment() {
    Log openLog;
    try (Log log = createLog(new LogConfig().withDirectory(UUID.randomUUID().toString()).withMaxEntriesPerSegment(128))) {
      openLog = log;
      checkInitial(log);
      for (int i = 0; i < 150; i++) {
        writeTestEntry(log);
      }
      checkLength(log, 150);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests that entries are not deduplicated before commitment.
   */
  public void testNoDedupeBeforeCommit() {
    Log openLog;
    try (Log log = createLog()) {
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
      .withDirectory("test-delete");
    Log log = createLog(config);
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
      .withDirectory(UUID.randomUUID().toString())
      .withMaxEntriesPerSegment(8);
    Log openLog;
    try (Log log = createLog(config)) {
      openLog = log;
      for (int i = 0; i < 16; i++) {
        writeTestEntry(log);
      }
      checkLength(log, 16);
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
      .withDirectory(UUID.randomUUID().toString())
      .withMaxEntriesPerSegment(8);
    Log openLog;
    try (Log log = createLog(config)) {
      writeTestEntry(log);
    }
    try (Log log = createLog(config)) {
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
      .withDirectory(UUID.randomUUID().toString())
      .withMaxEntriesPerSegment(8);
    Log openLog;
    try (Log log = createLog(config)) {
      for (int i = 0; i < 14; i++) {
        writeTestEntry(log);
      }
      readUncommittedKeyedTestEntry(log, 1);
      checkLength(log, 14);
    }
    try (Log log = createLog(config)) {
      openLog = log;
      readUncommittedKeyedTestEntry(log, 1);
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
      .withDirectory(UUID.randomUUID().toString())
      .withMaxEntriesPerSegment(8);
    Log openLog;
    try (Log log = createLog(config)) {
      for (int i = 0; i < 14; i++) {
        writeTestEntry(log);
      }
      checkLength(log, 14);
    }
    try (Log log = createLog(config)) {
      openLog = log;
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
      .withDirectory("test")
      .withMaxEntriesPerSegment(1024);
    ExecutionContext context = new ExecutionContext("test", new Serializer());

    try (Log log = new Log(new SegmentManager(config))) {
      log.open(context);
      Segment segment = log.segments.currentSegment();
      assertEquals(segment.descriptor().id(), 1);
      assertEquals(segment.descriptor().version(), 1);
      assertEquals(segment.descriptor().index(), 1);
      assertEquals(segment.descriptor().maxSegmentSize(), 1024);
    }

    try (Log log = new Log(new SegmentManager(config))) {
      log.open(context);
      Segment segment = log.segments.currentSegment();
      assertEquals(segment.descriptor().id(), 1);
      assertEquals(segment.descriptor().version(), 1);
      assertEquals(segment.descriptor().index(), 1);
      assertEquals(segment.descriptor().maxSegmentSize(), 1024);
      log.delete();
    }
  }

}
