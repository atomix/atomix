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
package net.kuujo.copycat.protocol.raft.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBuffer;
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
public class BufferedLogTest {
  private final Buffer KEY = HeapBuffer.allocate(1024);
  private final Buffer ENTRY = HeapBuffer.allocate(1024);

  /**
   * Creates a log with a default configuration.
   */
  private BufferedLog createLog() {
    return createLog(new LogConfig().withName(UUID.randomUUID().toString()));
  }

  /**
   * Creates a log with a custom configuration.
   */
  private BufferedLog createLog(LogConfig config) {
    BufferedLog log = new BufferedLog(config);
    log.open();
    return log;
  }

  /**
   * Performs assertions on an empty initialized log.
   */
  private void checkInitial(BufferedLog log) {
    assertTrue(log.isOpen());
    assertTrue(log.isEmpty());
    assertEquals(log.firstIndex(), 0);
    assertEquals(log.lastIndex(), 0);
    assertEquals(log.length(), 0);
  }

  /**
   * Writes a keyed test entry.
   */
  private void writeKeyedTestEntry(BufferedLog log) {
    long index = log.nextIndex();
    try (RaftEntry writer = log.createEntry()) {
      assertEquals(writer.index(), index);
      writer.writeType(RaftEntry.Type.COMMAND);
      writer.writeMode(RaftEntry.Mode.PERSISTENT);
      writer.writeTerm(1);
      writer.writeKey(KEY.clear().writeLong(1234).flip());
      writer.writeEntry(ENTRY.clear().writeLong(4321).flip());
    }
  }

  /**
   * Reads a keyed test entry.
   */
  private void readUncommittedKeyedTestEntry(BufferedLog log, long index) {
    try (RaftEntry reader = log.getEntry(index)) {
      assertEquals(reader.index(), index);
      assertEquals(reader.readMode(), RaftEntry.Mode.PERSISTENT);
      assertEquals(reader.readType(), RaftEntry.Type.COMMAND);
      assertEquals(reader.readTerm(), 1);
      reader.readKey(KEY.clear());
      reader.readEntry(ENTRY.clear());
      assertEquals(KEY.flip().readLong(), 1234);
      assertEquals(ENTRY.flip().readLong(), 4321);
    }
  }

  /**
   * Reads a keyed test entry.
   */
  private void readCommittedKeyedTestEntry(BufferedLog log, long index) {
    assertTrue(log.containsIndex(index));
    assertTrue(log.commitIndex() >= index);
    try (RaftEntry reader = log.getEntry(index)) {
      assertEquals(reader.index(), index);
      assertEquals(reader.readMode(), RaftEntry.Mode.PERSISTENT);
      assertEquals(reader.readType(), RaftEntry.Type.COMMAND);
      assertEquals(reader.readTerm(), 1);
      reader.readKey(KEY.clear());
      reader.readEntry(ENTRY.clear());
      assertEquals(KEY.flip().readLong(), 1234);
      assertEquals(ENTRY.flip().readLong(), 4321);
    }
  }

  /**
   * Reads a keyed entry that has been deduplicated.
   */
  private void readDeduplicatedTestEntry(BufferedLog log, long index) {
    assertTrue(log.containsIndex(index));
    assertFalse(log.containsEntry(index));
    try (RaftEntry reader = log.getEntry(index)) {
      assertNull(reader);
    }
  }

  /**
   * Checks the length of the log.
   */
  private void checkLength(BufferedLog log, long length) {
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
    BufferedLog openLog;
    try (BufferedLog log = createLog()) {
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
    BufferedLog openLog;
    try (BufferedLog log = createLog()) {
      openLog = log;
      checkInitial(log);
      writeKeyedTestEntry(log);
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
    BufferedLog openLog;
    try (BufferedLog log = createLog(new LogConfig().withName(UUID.randomUUID().toString()).withEntriesPerSegment(8))) {
      openLog = log;
      checkInitial(log);
      for (int i = 0; i < 12; i++) {
        writeKeyedTestEntry(log);
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
    BufferedLog openLog;
    try (BufferedLog log = createLog()) {
      openLog = log;
      checkInitial(log);
      writeKeyedTestEntry(log);
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
    BufferedLog openLog;
    try (BufferedLog log = createLog(new LogConfig().withName(UUID.randomUUID().toString()).withEntriesPerSegment(128))) {
      openLog = log;
      checkInitial(log);
      for (int i = 0; i < 150; i++) {
        writeKeyedTestEntry(log);
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
    BufferedLog openLog;
    try (BufferedLog log = createLog()) {
      openLog = log;
      checkInitial(log);
      writeKeyedTestEntry(log);
      checkLength(log, 1);
      writeKeyedTestEntry(log);
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
    BufferedLog log = createLog(config);
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
      .withEntriesPerSegment(8);
    BufferedLog openLog;
    try (BufferedLog log = createLog(config)) {
      openLog = log;
      for (int i = 0; i < 16; i++) {
        writeKeyedTestEntry(log);
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
      .withEntriesPerSegment(8);
    BufferedLog openLog;
    try (BufferedLog log = createLog(config)) {
      writeKeyedTestEntry(log);
    }
    try (BufferedLog log = createLog(config)) {
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
      .withEntriesPerSegment(8);
    BufferedLog openLog;
    try (BufferedLog log = createLog(config)) {
      for (int i = 0; i < 14; i++) {
        writeKeyedTestEntry(log);
      }
      readUncommittedKeyedTestEntry(log, 1);
      checkLength(log, 14);
    }
    try (BufferedLog log = createLog(config)) {
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
      .withEntriesPerSegment(8);
    BufferedLog openLog;
    try (BufferedLog log = createLog(config)) {
      for (int i = 0; i < 14; i++) {
        writeKeyedTestEntry(log);
      }
      checkLength(log, 14);
      log.commit(12);
      assertEquals(log.commitIndex(), 12);
    }
    try (BufferedLog log = createLog(config)) {
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
      .withEntriesPerSegment(1024)
      .withCompactInterval(10, TimeUnit.SECONDS);

    try (BufferedLog log = new BufferedLog(config)) {
      log.open();
      Segment segment = log.segments.currentSegment();
      assertEquals(segment.descriptor().id(), 1);
      assertEquals(segment.descriptor().version(), 1);
      assertEquals(segment.descriptor().index(), 1);
      assertEquals(segment.descriptor().entries(), 1024);
    }

    try (BufferedLog log = new BufferedLog(config)) {
      log.open();
      Segment segment = log.segments.currentSegment();
      assertEquals(segment.descriptor().id(), 1);
      assertEquals(segment.descriptor().version(), 1);
      assertEquals(segment.descriptor().index(), 1);
      assertEquals(segment.descriptor().entries(), 1024);
      log.delete();
    }
  }

}
