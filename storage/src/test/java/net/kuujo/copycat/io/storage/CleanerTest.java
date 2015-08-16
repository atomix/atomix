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
package net.kuujo.copycat.io.storage;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.ServiceLoaderTypeResolver;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Minor compaction test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class CleanerTest extends ConcurrentTestCase {

  /**
   * Tests compacting the log.
   */
  public void testCompact() throws Throwable {
    Storage storage = Storage.builder()
      .withStorageLevel(StorageLevel.MEMORY)
      .withMaxEntriesPerSegment(10)
      .withSerializer(new Serializer(new ServiceLoaderTypeResolver()))
      .build();

    Log log = storage.open();

    writeEntries(log, 30);

    assertEquals(log.length(), 30L);

    for (long index = 21; index < 28; index++) {
      log.cleanEntry(index);
    }

    expectResume();
    log.cleaner().clean().thenRun(this::resume);
    await();

    assertEquals(log.length(), 30L);

    for (long index = 21; index < 28; index++) {
      assertTrue(log.containsIndex(index));
      assertFalse(log.containsEntry(index));
      try (TestEntry entry = log.getEntry(index)) {
        assertNull(entry);
      }
    }
  }

  /**
   * Writes a set of session entries to the log.
   */
  private void writeEntries(Log log, int entries) {
    for (int i = 0; i < entries; i++) {
      try (TestEntry entry = log.createEntry(TestEntry.class)) {
        entry.setTerm(1);
        entry.setRemove(false);
        log.appendEntry(entry);
      }
    }
  }

}
