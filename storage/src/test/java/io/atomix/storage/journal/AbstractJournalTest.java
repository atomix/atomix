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

import io.atomix.storage.StorageLevel;
import io.atomix.utils.serializer.Namespace;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Base journal test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@RunWith(Parameterized.class)
public abstract class AbstractJournalTest {
  private static final Namespace NAMESPACE = Namespace.builder()
      .register(TestEntry.class)
      .register(byte[].class)
      .build();

  protected static final TestEntry ENTRY = new TestEntry(32);
  private static final Path PATH = Paths.get("target/test-logs/");

  private final int maxSegmentSize;
  private final int cacheSize;
  protected final int entriesPerSegment;

  protected AbstractJournalTest(int maxSegmentSize, int cacheSize) {
    this.maxSegmentSize = maxSegmentSize;
    this.cacheSize = cacheSize;
    int entryLength = (NAMESPACE.serialize(ENTRY).length + 8);
    this.entriesPerSegment = (maxSegmentSize - 64) / entryLength;
  }

  protected abstract StorageLevel storageLevel();

  @Parameterized.Parameters
  public static Collection primeNumbers() {
    List<Object[]> runs = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 10; j++) {
        runs.add(new Object[]{64 + (i * (NAMESPACE.serialize(ENTRY).length + 8) + j), j});
      }
    }
    return runs;
  }

  protected SegmentedJournal<TestEntry> createJournal() {
    return SegmentedJournal.<TestEntry>builder()
        .withName("test")
        .withDirectory(PATH.toFile())
        .withNamespace(NAMESPACE)
        .withStorageLevel(storageLevel())
        .withMaxSegmentSize(maxSegmentSize)
        .withIndexDensity(.2)
        .withCacheSize(cacheSize)
        .build();
  }

  @Test
  public void testCloseMultipleTimes() {
    // given
    final Journal<TestEntry> journal = createJournal();

    // when
    journal.close();

    // then
    journal.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteRead() throws Exception {
    try (Journal<TestEntry> journal = createJournal()) {
      JournalWriter<TestEntry> writer = journal.writer();
      JournalReader<TestEntry> reader = journal.openReader(1);

      // Append a couple entries.
      Indexed<TestEntry> indexed;
      assertEquals(1, writer.getNextIndex());
      indexed = writer.append(ENTRY);
      assertEquals(1, indexed.index());

      assertEquals(2, writer.getNextIndex());
      writer.append(new Indexed<>(2, ENTRY, 0));
      reader.reset(2);
      indexed = reader.next();
      assertEquals(2, indexed.index());
      assertFalse(reader.hasNext());

      // Test reading an entry
      Indexed<TestEntry> entry1;
      reader.reset();
      entry1 = (Indexed) reader.next();
      assertEquals(1, entry1.index());
      assertEquals(entry1, reader.getCurrentEntry());
      assertEquals(1, reader.getCurrentIndex());

      // Test reading a second entry
      Indexed<TestEntry> entry2;
      assertTrue(reader.hasNext());
      assertEquals(2, reader.getNextIndex());
      entry2 = (Indexed) reader.next();
      assertEquals(2, entry2.index());
      assertEquals(entry2, reader.getCurrentEntry());
      assertEquals(2, reader.getCurrentIndex());
      assertFalse(reader.hasNext());

      // Test opening a new reader and reading from the journal.
      reader = journal.openReader(1);
      assertTrue(reader.hasNext());
      entry1 = (Indexed) reader.next();
      assertEquals(1, entry1.index());
      assertEquals(entry1, reader.getCurrentEntry());
      assertEquals(1, reader.getCurrentIndex());
      assertTrue(reader.hasNext());

      assertTrue(reader.hasNext());
      assertEquals(2, reader.getNextIndex());
      entry2 = (Indexed) reader.next();
      assertEquals(2, entry2.index());
      assertEquals(entry2, reader.getCurrentEntry());
      assertEquals(2, reader.getCurrentIndex());
      assertFalse(reader.hasNext());

      // Reset the reader.
      reader.reset();

      // Test opening a new reader and reading from the journal.
      reader = journal.openReader(1);
      assertTrue(reader.hasNext());
      entry1 = (Indexed) reader.next();
      assertEquals(1, entry1.index());
      assertEquals(entry1, reader.getCurrentEntry());
      assertEquals(1, reader.getCurrentIndex());
      assertTrue(reader.hasNext());

      assertTrue(reader.hasNext());
      assertEquals(2, reader.getNextIndex());
      entry2 = (Indexed) reader.next();
      assertEquals(2, entry2.index());
      assertEquals(entry2, reader.getCurrentEntry());
      assertEquals(2, reader.getCurrentIndex());
      assertFalse(reader.hasNext());

      // Truncate the journal and write a different entry.
      writer.truncate(1);
      assertEquals(2, writer.getNextIndex());
      writer.append(new Indexed<>(2, ENTRY, 0));
      reader.reset(2);
      indexed = reader.next();
      assertEquals(2, indexed.index());

      // Reset the reader to a specific index and read the last entry again.
      reader.reset(2);

      assertNotNull(reader.getCurrentEntry());
      assertEquals(1, reader.getCurrentIndex());
      assertEquals(1, reader.getCurrentEntry().index());
      assertTrue(reader.hasNext());
      assertEquals(2, reader.getNextIndex());
      entry2 = (Indexed) reader.next();
      assertEquals(2, entry2.index());
      assertEquals(entry2, reader.getCurrentEntry());
      assertEquals(2, reader.getCurrentIndex());
      assertFalse(reader.hasNext());
    }
  }

  @Test
  public void testResetTruncateZero() throws Exception {
    try (SegmentedJournal<TestEntry> journal = createJournal()) {
      JournalWriter<TestEntry> writer = journal.writer();
      JournalReader<TestEntry> reader = journal.openReader(1);

      assertEquals(0, writer.getLastIndex());
      writer.append(ENTRY);
      writer.append(ENTRY);
      writer.reset(1);
      assertEquals(0, writer.getLastIndex());
      writer.append(ENTRY);
      assertEquals(1, reader.next().index());
      writer.reset(1);
      assertEquals(0, writer.getLastIndex());
      writer.append(ENTRY);
      assertEquals(1, writer.getLastIndex());
      assertEquals(1, writer.getLastEntry().index());

      assertTrue(reader.hasNext());
      assertEquals(1, reader.next().index());

      writer.truncate(0);
      assertEquals(0, writer.getLastIndex());
      assertNull(writer.getLastEntry());
      writer.append(ENTRY);
      assertEquals(1, writer.getLastIndex());
      assertEquals(1, writer.getLastEntry().index());

      assertTrue(reader.hasNext());
      assertEquals(1, reader.next().index());
    }
  }

  @Test
  public void testTruncateRead() throws Exception {
    int i = 10;
    try (Journal<TestEntry> journal = createJournal()) {
      JournalWriter<TestEntry> writer = journal.writer();
      JournalReader<TestEntry> reader = journal.openReader(1);

      for (int j = 1; j <= i; j++) {
        assertEquals(j, writer.append(new TestEntry(32)).index());
      }

      for (int j = 1; j <= i - 2; j++) {
        assertTrue(reader.hasNext());
        assertEquals(j, reader.next().index());
      }

      writer.truncate(i - 2);

      assertFalse(reader.hasNext());
      assertEquals(i - 1, writer.append(new TestEntry(32)).index());
      assertEquals(i, writer.append(new TestEntry(32)).index());

      assertTrue(reader.hasNext());
      Indexed<TestEntry> entry = reader.next();
      assertEquals(i - 1, entry.index());
      assertTrue(reader.hasNext());
      entry = reader.next();
      assertEquals(i, entry.index());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteReadEntries() throws Exception {
    try (Journal<TestEntry> journal = createJournal()) {
      JournalWriter<TestEntry> writer = journal.writer();
      JournalReader<TestEntry> reader = journal.openReader(1);

      for (int i = 1; i <= entriesPerSegment * 5; i++) {
        writer.append(ENTRY);
        assertTrue(reader.hasNext());
        Indexed<TestEntry> entry;
        entry = (Indexed) reader.next();
        assertEquals(i, entry.index());
        assertEquals(32, entry.entry().bytes().length);
        reader.reset(i);
        entry = (Indexed) reader.next();
        assertEquals(i, entry.index());
        assertEquals(32, entry.entry().bytes().length);

        if (i > 6) {
          reader.reset(i - 5);
          assertNotNull(reader.getCurrentEntry());
          assertEquals(i - 6, reader.getCurrentIndex());
          assertEquals(i - 6, reader.getCurrentEntry().index());
          assertEquals(i - 5, reader.getNextIndex());
          reader.reset(i + 1);
        }

        writer.truncate(i - 1);
        writer.append(ENTRY);

        assertTrue(reader.hasNext());
        reader.reset(i);
        assertTrue(reader.hasNext());
        entry = (Indexed) reader.next();
        assertEquals(i, entry.index());
        assertEquals(32, entry.entry().bytes().length);
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWriteReadCommittedEntries() throws Exception {
    try (Journal<TestEntry> journal = createJournal()) {
      JournalWriter<TestEntry> writer = journal.writer();
      JournalReader<TestEntry> reader = journal.openReader(1, JournalReader.Mode.COMMITS);

      for (int i = 1; i <= entriesPerSegment * 5; i++) {
        writer.append(ENTRY);
        assertFalse(reader.hasNext());
        writer.commit(i);
        assertTrue(reader.hasNext());
        Indexed<TestEntry> entry;
        entry = (Indexed) reader.next();
        assertEquals(i, entry.index());
        assertEquals(32, entry.entry().bytes().length);
        reader.reset(i);
        entry = (Indexed) reader.next();
        assertEquals(i, entry.index());
        assertEquals(32, entry.entry().bytes().length);
      }
    }
  }

  @Test
  public void testReadAfterCompact() throws Exception {
    try (SegmentedJournal<TestEntry> journal = createJournal()) {
      JournalWriter<TestEntry> writer = journal.writer();
      JournalReader<TestEntry> uncommittedReader = journal.openReader(1, JournalReader.Mode.ALL);
      JournalReader<TestEntry> committedReader = journal.openReader(1, JournalReader.Mode.COMMITS);

      for (int i = 1; i <= entriesPerSegment * 10; i++) {
        assertEquals(i, writer.append(ENTRY).index());
      }

      assertEquals(1, uncommittedReader.getNextIndex());
      assertTrue(uncommittedReader.hasNext());
      assertEquals(1, committedReader.getNextIndex());
      assertFalse(committedReader.hasNext());

      writer.commit(entriesPerSegment * 9);

      assertTrue(uncommittedReader.hasNext());
      assertTrue(committedReader.hasNext());

      for (int i = 1; i <= entriesPerSegment * 2.5; i++) {
        assertEquals(i, uncommittedReader.next().index());
        assertEquals(i, committedReader.next().index());
      }

      journal.compact(entriesPerSegment * 5 + 1);

      assertNull(uncommittedReader.getCurrentEntry());
      assertEquals(0, uncommittedReader.getCurrentIndex());
      assertTrue(uncommittedReader.hasNext());
      assertEquals(entriesPerSegment * 5 + 1, uncommittedReader.getNextIndex());
      assertEquals(entriesPerSegment * 5 + 1, uncommittedReader.next().index());

      assertNull(committedReader.getCurrentEntry());
      assertEquals(0, committedReader.getCurrentIndex());
      assertTrue(committedReader.hasNext());
      assertEquals(entriesPerSegment * 5 + 1, committedReader.getNextIndex());
      assertEquals(entriesPerSegment * 5 + 1, committedReader.next().index());
    }
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
