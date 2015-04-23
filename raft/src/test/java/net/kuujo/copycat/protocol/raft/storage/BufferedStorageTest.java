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
public class BufferedStorageTest {
  private final Buffer KEY = HeapBuffer.allocate(1024);
  private final Buffer ENTRY = HeapBuffer.allocate(1024);

  /**
   * Creates a log with a default configuration.
   */
  private BufferedStorage createLog() {
    return createLog(new StorageConfig().withName(UUID.randomUUID().toString()));
  }

  /**
   * Creates a log with a custom configuration.
   */
  private BufferedStorage createLog(StorageConfig config) {
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
    assertEquals(log.size(), 0);
    assertEquals(log.length(), 0);
  }

  /**
   * Writes a keyed test entry.
   */
  private void writeKeyedTestEntry(BufferedStorage log) {
    long index = log.nextIndex();
    try (RaftEntry writer = log.createEntry()) {
      assertEquals(writer.index(), index);
      writer.writeType(RaftEntry.Type.COMMAND);
      writer.writeTerm(1);
      writer.writeKey(KEY.clear().writeLong(1234).flip());
      writer.writeEntry(ENTRY.clear().writeLong(4321).flip());
    }
  }

  /**
   * Reads a keyed test entry.
   */
  private void readUncommittedKeyedTestEntry(BufferedStorage log, long index) {
    try (RaftEntry reader = log.getEntry(index)) {
      assertEquals(reader.index(), index);
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
  private void readCommittedKeyedTestEntry(BufferedStorage log, long index) {
    assertTrue(log.containsIndex(index));
    assertTrue(log.commitIndex() >= index);
    try (RaftEntry reader = log.getEntry(index)) {
      assertEquals(reader.index(), index);
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
  private void readDeduplicatedTestEntry(BufferedStorage log, long index) {
    assertTrue(log.containsIndex(index));
    assertFalse(log.containsEntry(index));
    try (RaftEntry reader = log.getEntry(index)) {
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
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(new StorageConfig().withName(UUID.randomUUID().toString()).withEntriesPerSegment(8))) {
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
    BufferedStorage openLog;
    try (BufferedStorage log = createLog()) {
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
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(new StorageConfig().withName(UUID.randomUUID().toString()).withEntriesPerSegment(128))) {
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
    BufferedStorage openLog;
    try (BufferedStorage log = createLog()) {
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
   * Tests that entries are deduplicated after commitment.
   */
  public void testDedupeAfterCommit() {
    BufferedStorage openLog;
    try (BufferedStorage log = createLog()) {
      openLog = log;
      checkInitial(log);
      writeKeyedTestEntry(log);
      checkLength(log, 1);
      writeKeyedTestEntry(log);
      checkLength(log, 2);
      log.commit(1);
      assertEquals(log.commitIndex(), 1);
      readCommittedKeyedTestEntry(log, 1);
      readUncommittedKeyedTestEntry(log, 2);
      log.commit(2);
      assertEquals(log.commitIndex(), 2);
      readDeduplicatedTestEntry(log, 1);
      readCommittedKeyedTestEntry(log, 2);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests deleting a log.
   */
  public void testDelete() {
    StorageConfig config = new StorageConfig()
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
    StorageConfig config = new StorageConfig()
      .withName(UUID.randomUUID().toString())
      .withCompactInterval(Long.MAX_VALUE)
      .withEntriesPerSegment(8);
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(config)) {
      openLog = log;
      for (int i = 0; i < 16; i++) {
        writeKeyedTestEntry(log);
      }
      checkLength(log, 16);
      log.commit(12);
      assertEquals(log.commitIndex(), 12);
      log.compact();
      readCommittedKeyedTestEntry(log, 8);
    }
    assertFalse(openLog.isOpen());
    openLog.delete();
  }

  /**
   * Tests reading an entry from the log after recovering.
   */
  public void testRecoverRead() {
    StorageConfig config = new StorageConfig()
      .withName(UUID.randomUUID().toString())
      .withCompactInterval(Long.MAX_VALUE)
      .withEntriesPerSegment(8);
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(config)) {
      writeKeyedTestEntry(log);
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
    StorageConfig config = new StorageConfig()
      .withName(UUID.randomUUID().toString())
      .withCompactInterval(Long.MAX_VALUE)
      .withEntriesPerSegment(8);
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(config)) {
      for (int i = 0; i < 14; i++) {
        writeKeyedTestEntry(log);
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
    StorageConfig config = new StorageConfig()
      .withName(UUID.randomUUID().toString())
      .withCompactInterval(Long.MAX_VALUE)
      .withEntriesPerSegment(8);
    BufferedStorage openLog;
    try (BufferedStorage log = createLog(config)) {
      for (int i = 0; i < 14; i++) {
        writeKeyedTestEntry(log);
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
    StorageConfig config = new StorageConfig()
      .withName("test")
      .withEntriesPerSegment(1024)
      .withCompactInterval(10, TimeUnit.SECONDS);

    try (BufferedStorage log = new BufferedStorage(config)) {
      log.open();
      Segment segment = log.segments.currentSegment();
      assertEquals(segment.descriptor().id(), 1);
      assertEquals(segment.descriptor().version(), 1);
      assertEquals(segment.descriptor().index(), 1);
      assertEquals(segment.descriptor().entries(), 1024);
    }

    try (BufferedStorage log = new BufferedStorage(config)) {
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
