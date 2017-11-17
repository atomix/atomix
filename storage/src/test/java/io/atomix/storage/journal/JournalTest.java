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

import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.storage.StorageLevel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class JournalTest {
  private static final Serializer serializer = Serializer.using(KryoNamespace.builder()
      .register(TestEntry.class)
      .register(byte[].class)
      .build());

  private Journal<TestEntry> createJournal() {
    return SegmentedJournal.<TestEntry>builder()
        .withName("test")
        .withSerializer(serializer)
        .withStorageLevel(StorageLevel.MEMORY)
        .build();
  }

  @Test
  public void testLogWriteRead() throws Exception {
    Journal<TestEntry> journal = createJournal();
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
}