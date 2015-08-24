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

import org.testng.annotations.Test;

import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.ServiceLoaderTypeResolver;

import static org.testng.Assert.*;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class LogTest extends AbstractLogTest {
  /**
   * Creates a new log.
   */
  @Override
  protected Log createLog() {
    return tempStorageBuilder().withMaxEntrySize(1024)
        .withMaxSegmentSize(1024 * 1024)
        .withMaxEntriesPerSegment(1024)
        .withSerializer(new Serializer(new ServiceLoaderTypeResolver()))
        .build()
        .open();
  }

  /**
   * Tests writing and reading an entry.
   */
  public void testCreateReadFirstEntry() {
    assertTrue(log.isEmpty());
    assertEquals(log.length(), 0);

    long index;
    try (TestEntry entry = log.create(TestEntry.class)) {
      entry.setTerm(1);
      entry.setRemove(true);
      index = log.append(entry);
    }

    assertEquals(log.length(), 1);
    assertFalse(log.isEmpty());

    try (TestEntry entry = log.get(index)) {
      assertEquals(entry.getTerm(), 1);
      assertTrue(entry.isRemove());
    }
  }

  /**
   * Tests creating and reading the last entry in the log.
   */
  public void testCreateReadLastEntry() {
    appendEntries(log, 100);
    assertEquals(log.length(), 100);

    long index;
    try (TestEntry entry = log.create(TestEntry.class)) {
      entry.setTerm(1);
      entry.setRemove(true);
      index = log.append(entry);
    }

    assertEquals(log.length(), 101);

    try (TestEntry entry = log.get(index)) {
      assertEquals(entry.getTerm(), 1);
      assertTrue(entry.isRemove());
    }
  }

  /**
   * Tests creating and reading the last entry in the log.
   */
  public void testCreateReadMiddleEntry() {
    appendEntries(log, 100);
    assertEquals(log.length(), 100);

    long index;
    try (TestEntry entry = log.create(TestEntry.class)) {
      entry.setTerm(1);
      entry.setRemove(true);
      index = log.append(entry);
    }

    appendEntries(log, 100);
    assertEquals(log.length(), 201);

    try (TestEntry entry = log.get(index)) {
      assertEquals(entry.getTerm(), 1);
      assertTrue(entry.isRemove());
    }
  }

  /**
   * Tests creating and reading entries after a roll over.
   */
  public void testCreateReadAfterRollOver() {
    appendEntries(log, 1100);

    long index;
    try (TestEntry entry = log.create(TestEntry.class)) {
      entry.setTerm(1);
      entry.setRemove(true);
      index = log.append(entry);
    }

    appendEntries(log, 1050);

    try (TestEntry entry = log.get(index)) {
      assertEquals(entry.getTerm(), 1);
      assertTrue(entry.isRemove());
    }
  }

  /**
   * Tests truncating entries in the log.
   */
  public void testTruncate() throws Throwable {
    appendEntries(log, 100);
    assertEquals(log.lastIndex(), 100);
    log.truncate(10);
    assertEquals(log.lastIndex(), 10);
    appendEntries(log, 10);
    assertEquals(log.lastIndex(), 20);
  }

  /**
   * Tests emptying the log.
   */
  public void testTruncateZero() throws Throwable {
    appendEntries(log, 100);
    assertEquals(log.lastIndex(), 100);
    log.truncate(0);
    assertEquals(log.lastIndex(), 0);
    appendEntries(log, 10);
    assertEquals(log.lastIndex(), 10);
  }

  /**
   * Tests skipping entries in the log.
   */
  public void testSkip() throws Throwable {
    appendEntries(log, 100);

    log.skip(10);

    long index;
    try (TestEntry entry = log.create(TestEntry.class)) {
      entry.setTerm(1);
      entry.setRemove(true);
      index = log.append(entry);
    }

    assertEquals(log.length(), 111);

    try (TestEntry entry = log.get(101)) {
      assertNull(entry);
    }

    try (TestEntry entry = log.get(index)) {
      assertEquals(entry.getTerm(), 1);
      assertTrue(entry.isRemove());
    }
  }

  /**
   * Tests skipping entries on a segment rollover.
   */
  public void testSkipOnRollOver() {
    appendEntries(log, 1020);

    log.skip(10);

    assertEquals(log.length(), 1030);

    long index;
    try (TestEntry entry = log.create(TestEntry.class)) {
      entry.setTerm(1);
      entry.setRemove(true);
      index = log.append(entry);
    }

    assertEquals(log.length(), 1031);

    try (TestEntry entry = log.get(1021)) {
      assertNull(entry);
    }

    try (TestEntry entry = log.get(index)) {
      assertEquals(entry.getTerm(), 1);
      assertTrue(entry.isRemove());
    }
  }

  /**
   * Tests recovering the log.
   */
  public void testRecover() {
    appendEntries(log, 1024);
    assertEquals(log.length(), 1024);

    try (Log log = createLog()) {
      assertEquals(log.length(), 1024);

      for (long i = log.firstIndex(); i <= log.lastIndex(); i++) {
        try (Entry entry = log.get(i)) {
          assertNotNull(entry);
        }
      }
    }
  }

  /**
   * Tests recovering the log after compaction.
   */
  public void testRecoverAfterCompact() {
    appendEntries(log, 2048);
    for (long i = 1; i <= 2048; i++) {
      if (i % 3 == 0 || i % 3 == 1) {
        log.clean(i);
      }
    }

    for (long i = 1; i <= 2048; i++) {
      if (i % 3 == 0 || i % 3 == 1) {
        assertTrue(log.lastIndex() >= i);
        assertFalse(log.contains(i));
      }
    }
    log.cleaner().clean().join();

    try (Log log = createLog()) {
      assertEquals(log.length(), 2048);
      for (long i = 1; i <= 2048; i++) {
        if (i % 3 == 0 || i % 3 == 1) {
          assertTrue(log.lastIndex() >= i);
          assertFalse(log.contains(i));
          assertNull(log.get(i));
        }
      }
    }
  }

  /**
   * Appends a set of entries to the log.
   */
  private void appendEntries(Log log, int entries) {
    for (int i = 0; i < entries; i++) {
      try (TestEntry entry = log.create(TestEntry.class)) {
        entry.setTerm(1);
        entry.setRemove(true);
        log.append(entry);
      }
    }
  }
}
