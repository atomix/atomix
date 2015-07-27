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
package net.kuujo.copycat.log;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.ServiceLoaderResolver;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.SingleThreadContext;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class LogTest {

  /**
   * Tests writing and reading an entry.
   */
  public void testCreateReadFirstEntry() {
    try (Log log = createLog()) {
      Assert.assertTrue(log.isEmpty());
      Assert.assertEquals(log.length(), 0);

      long index;
      try (TestEntry entry = log.createEntry(TestEntry.class)) {
        entry.setTerm(1);
        entry.setRemove(true);
        index = log.appendEntry(entry);
      }

      Assert.assertEquals(log.length(), 1);
      Assert.assertFalse(log.isEmpty());

      try (TestEntry entry = log.getEntry(index)) {
        Assert.assertEquals(entry.getTerm(), 1);
        Assert.assertTrue(entry.isRemove());
      }
    }
  }

  /**
   * Tests creating and reading the last entry in the log.
   */
  public void testCreateReadLastEntry() {
    try (Log log = createLog()) {
      appendEntries(log, 100);
      Assert.assertEquals(log.length(), 100);

      long index;
      try (TestEntry entry = log.createEntry(TestEntry.class)) {
        entry.setTerm(1);
        entry.setRemove(true);
        index = log.appendEntry(entry);
      }

      Assert.assertEquals(log.length(), 101);

      try (TestEntry entry = log.getEntry(index)) {
        Assert.assertEquals(entry.getTerm(), 1);
        Assert.assertTrue(entry.isRemove());
      }
    }
  }

  /**
   * Tests creating and reading the last entry in the log.
   */
  public void testCreateReadMiddleEntry() {
    try (Log log = createLog()) {
      appendEntries(log, 100);
      Assert.assertEquals(log.length(), 100);

      long index;
      try (TestEntry entry = log.createEntry(TestEntry.class)) {
        entry.setTerm(1);
        entry.setRemove(true);
        index = log.appendEntry(entry);
      }

      appendEntries(log, 100);
      Assert.assertEquals(log.length(), 201);

      try (TestEntry entry = log.getEntry(index)) {
        Assert.assertEquals(entry.getTerm(), 1);
        Assert.assertTrue(entry.isRemove());
      }
    }
  }

  /**
   * Tests creating and reading entries after a roll over.
   */
  public void testCreateReadAfterRollOver() {
    try (Log log = createLog()) {
      appendEntries(log, 1100);

      long index;
      try (TestEntry entry = log.createEntry(TestEntry.class)) {
        entry.setTerm(1);
        entry.setRemove(true);
        index = log.appendEntry(entry);
      }

      appendEntries(log, 1050);

      try (TestEntry entry = log.getEntry(index)) {
        Assert.assertEquals(entry.getTerm(), 1);
        Assert.assertTrue(entry.isRemove());
      }
    }
  }

  /**
   * Tests truncating entries in the log.
   */
  public void testTruncate() throws Throwable {
    try (Log log = createLog()) {
      appendEntries(log, 100);
      Assert.assertEquals(log.lastIndex(), 100);
      log.truncate(10);
      Assert.assertEquals(log.lastIndex(), 10);
      appendEntries(log, 10);
      Assert.assertEquals(log.lastIndex(), 20);
    }
  }

  /**
   * Tests skipping entries in the log.
   */
  public void testSkip() throws Throwable {
    try (Log log = createLog()) {
      appendEntries(log, 100);

      log.skip(10);

      long index;
      try (TestEntry entry = log.createEntry(TestEntry.class)) {
        entry.setTerm(1);
        entry.setRemove(true);
        index = log.appendEntry(entry);
      }

      Assert.assertEquals(log.length(), 111);

      try (TestEntry entry = log.getEntry(101)) {
        Assert.assertNull(entry);
      }

      try (TestEntry entry = log.getEntry(index)) {
        Assert.assertEquals(entry.getTerm(), 1);
        Assert.assertTrue(entry.isRemove());
      }
    }
  }

  /**
   * Tests skipping entries on a segment rollover.
   */
  public void testSkipOnRollOver() {
    try (Log log = createLog()) {
      appendEntries(log, 1020);

      log.skip(10);

      Assert.assertEquals(log.length(), 1030);

      long index;
      try (TestEntry entry = log.createEntry(TestEntry.class)) {
        entry.setTerm(1);
        entry.setRemove(true);
        index = log.appendEntry(entry);
      }

      Assert.assertEquals(log.length(), 1031);

      try (TestEntry entry = log.getEntry(1021)) {
        Assert.assertNull(entry);
      }

      try (TestEntry entry = log.getEntry(index)) {
        Assert.assertEquals(entry.getTerm(), 1);
        Assert.assertTrue(entry.isRemove());
      }
    }
  }

  /**
   * Creates a test execution context.
   */
  private Context createContext() {
    return new SingleThreadContext("test", new Alleycat(new ServiceLoaderResolver()));
  }

  /**
   * Creates a new in-memory log.
   */
  private Log createLog() {
    Log log = Log.builder()
      .withStorageLevel(StorageLevel.MEMORY)
      .withMaxEntrySize(1024)
      .withMaxSegmentSize(1024 * 1024)
      .withMaxEntriesPerSegment(1024)
      .build();
    log.open(createContext());
    Assert.assertTrue(log.isOpen());
    return log;
  }

  /**
   * Appends a set of entries to the log.
   */
  private void appendEntries(Log log, int entries) {
    for (int i = 0; i < entries; i++) {
      try (TestEntry entry = log.createEntry(TestEntry.class)) {
        entry.setTerm(1);
        entry.setRemove(true);
        log.appendEntry(entry);
      }
    }
  }

}
