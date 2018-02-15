/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.storage.journal;

import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.storage.StorageLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class JournalTest {
  private static final Path PATH = Paths.get("target/test-logs/");
  private static final Serializer serializer = Serializer.using(KryoNamespace.newBuilder()
      .register(TestEntry.class)
      .register(byte[].class)
      .build());

  private SegmentedJournal<TestEntry> createJournal(StorageLevel storageLevel) {
    return SegmentedJournal.<TestEntry>newBuilder()
        .withName("test")
        .withDirectory(PATH.toFile())
        .withSerializer(serializer)
        .withStorageLevel(storageLevel)
        .build();
  }

  @Test
  public void testLogWriteRead() throws Exception {
    Journal<TestEntry> journal = createJournal(StorageLevel.MEMORY);
    JournalWriter<TestEntry> writer = journal.writer();
    JournalReader<TestEntry> reader = journal.openReader(1);

    // Append a couple entries.
    Indexed<TestEntry> indexed;
    assertEquals(writer.getNextIndex(), 1);
    indexed = writer.append(new TestEntry(32));
    assertEquals(indexed.index(), 1);

    assertEquals(writer.getNextIndex(), 2);
    writer.append(new Indexed<>(2, new TestEntry(32), 32));
    reader.reset(2);
    indexed = reader.next();
    assertEquals(indexed.index(), 2);
    assertFalse(reader.hasNext());

    // Test reading the register entry.
    Indexed<TestEntry> openSession;
    reader.reset();
    openSession = reader.next();
    assertEquals(openSession.index(), 1);
    assertEquals(reader.getCurrentEntry(), openSession);
    assertEquals(reader.getCurrentIndex(), 1);

    // Test reading the unregister entry.
    Indexed<TestEntry> closeSession;
    assertTrue(reader.hasNext());
    assertEquals(reader.getNextIndex(), 2);
    closeSession = reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(reader.getCurrentEntry(), closeSession);
    assertEquals(reader.getCurrentIndex(), 2);
    assertFalse(reader.hasNext());

    // Test opening a new reader and reading from the log.
    reader = journal.openReader(1);
    assertTrue(reader.hasNext());
    openSession = reader.next();
    assertEquals(openSession.index(), 1);
    assertEquals(reader.getCurrentEntry(), openSession);
    assertEquals(reader.getCurrentIndex(), 1);
    assertTrue(reader.hasNext());

    assertTrue(reader.hasNext());
    assertEquals(reader.getNextIndex(), 2);
    closeSession = reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(reader.getCurrentEntry(), closeSession);
    assertEquals(reader.getCurrentIndex(), 2);
    assertFalse(reader.hasNext());

    // Reset the reader.
    reader.reset();

    // Test opening a new reader and reading from the log.
    reader = journal.openReader(1);
    assertTrue(reader.hasNext());
    openSession = reader.next();
    assertEquals(openSession.index(), 1);
    assertEquals(reader.getCurrentEntry(), openSession);
    assertEquals(reader.getCurrentIndex(), 1);
    assertTrue(reader.hasNext());

    assertTrue(reader.hasNext());
    assertEquals(reader.getNextIndex(), 2);
    closeSession = reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(reader.getCurrentEntry(), closeSession);
    assertEquals(reader.getCurrentIndex(), 2);
    assertFalse(reader.hasNext());

    // Truncate the log and write a different entry.
    writer.truncate(1);
    assertEquals(writer.getNextIndex(), 2);
    writer.append(new Indexed<>(2, new TestEntry(32), 32));
    reader.reset(2);
    indexed = reader.next();
    assertEquals(indexed.index(), 2);

    // Reset the reader to a specific index and read the last entry again.
    reader.reset(2);

    assertTrue(reader.hasNext());
    assertEquals(reader.getNextIndex(), 2);
    closeSession = reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(reader.getCurrentEntry(), closeSession);
    assertEquals(reader.getCurrentIndex(), 2);
    assertFalse(reader.hasNext());
  }

  @Test
  public void testLoadSegments() throws Exception {
    SegmentedJournal<TestEntry> journal = createJournal(StorageLevel.DISK);
    JournalSegment<TestEntry> segment1 = journal.createSegment(JournalSegmentDescriptor.newBuilder()
        .withId(1)
        .withIndex(1)
        .withMaxEntries(10)
        .withMaxSegmentSize(1024)
        .build());
    segment1.writer().append(new TestEntry(32));
    JournalSegment<TestEntry> segment2 = journal.createSegment(JournalSegmentDescriptor.newBuilder()
        .withId(2)
        .withIndex(10)
        .withMaxEntries(10)
        .withMaxSegmentSize(1024)
        .build());
    segment2.writer().append(new TestEntry(32));
    segment1.close();
    segment2.close();
    journal.close();

    journal = createJournal(StorageLevel.DISK);
    JournalReader<TestEntry> reader = journal.openReader(1);
    assertEquals(1, reader.getNextIndex());
    assertTrue(reader.hasNext());
    assertEquals(1, reader.next().index());
    assertFalse(reader.hasNext());
  }

  @Before
  @After
  public void cleanupStorage() throws IOException {
    if (Files.exists(PATH)) {
      Files.walkFileTree(PATH, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    }
  }
}