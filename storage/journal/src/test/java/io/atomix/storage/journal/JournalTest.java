/*
 * Copyright 2017-present Open Networking Laboratory
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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class JournalTest {

  private Journal<TestEntry> createJournal() {
    return SegmentedJournal.newBuilder()
        .withName("test")
        .withStorageLevel(StorageLevel.MEMORY)
        .build();
  }

  public void testJournalWriteRead() throws Exception {
    Journal<TestEntry> journal = createJournal();
    JournalWriter<TestEntry> writer = journal.writer();
    JournalReader<TestEntry> reader = journal.openReader(1);

    // Append a couple entries.
    Indexed<TestEntry> indexed;
    assertEquals(writer.getNextIndex(), 1);
    indexed = writer.append(new TestEntry(10));
    assertEquals(indexed.index(), 1);

    assertEquals(writer.getNextIndex(), 2);
    writer.append(new Indexed<>(2, new TestEntry(10), 0));

    reader.reset(2);
    indexed = reader.next();
    assertEquals(indexed.index(), 2);
  }
}