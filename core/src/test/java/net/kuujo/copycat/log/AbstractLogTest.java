/*
 * Copyright 2014 the original author or authors.
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

import net.kuujo.copycat.internal.util.Bytes;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.*;

/**
 * Tests log implementations.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 * @author Jonathan Halterman
 */
@Test
public abstract class AbstractLogTest {
  protected AbstractLog log;
  protected int segmentSize = 100;
  protected int entriesPerSegment = (segmentSize / entrySize()) + 1;

  /**
   * Creates a test log instance.
   */
  protected abstract AbstractLog createLog() throws Throwable;

  /**
   * Deletes the test log instance.
   */
  protected void deleteLog() throws Throwable {
  }

  /** Returns the size of a simple entry */
  protected abstract int entrySize();

  @BeforeMethod
  protected void beforeMethod() throws Throwable {
    log = createLog();
    assertTrue(log.isClosed());
    assertFalse(log.isOpen());
    log.open();
    assertTrue(log.isOpen());
    assertFalse(log.isClosed());
    assertTrue(log.isEmpty());
  }

  @AfterMethod
  protected void afterMethod() throws Throwable {
    try {
      log.close();
      assertFalse(log.isOpen());
      assertTrue(log.isClosed());
    } catch (Exception ignore) {
    } finally {
      log.delete();
    }
  }

  public void testAppendEntries() throws Exception {
    assertEquals(log.appendEntries(Arrays.asList(Bytes.of("1"), Bytes.of("2"), Bytes.of("3"))),
      Arrays.asList(1L, 2L, 3L));
  }

  public void testAppendEntry() throws Exception {
    assertEquals(log.appendEntry(Bytes.of("1")), 1L);
    assertEquals(log.appendEntry(Bytes.of("2")), 2L);
    assertEquals(log.appendEntry(Bytes.of("3")), 3L);
  }

  /**
   * Tests appending and getting entries.
   */
  public void testAppendGetEntries() {
    appendEntries(5);
    assertFalse(log.isEmpty());
    assertFalse(log.containsIndex(0));
    assertTrue(log.containsIndex(1));
    assertBytesEqual(log.getEntry(1), 1);
    assertBytesEqual(log.getEntry(2), 2);
    assertBytesEqual(log.getEntry(3), 3);
    assertBytesEqual(log.getEntry(4), 4);
    assertBytesEqual(log.getEntry(5), 5);
    assertFalse(log.containsIndex(6));
    log.appendEntry(Bytes.of("6"));
    log.appendEntry(Bytes.of("7"));
    log.appendEntry(Bytes.of("8"));
    log.appendEntry(Bytes.of("9"));
    log.appendEntry(Bytes.of("10"));
    assertTrue(log.containsIndex(10));
    assertFalse(log.containsIndex(11));
    List<ByteBuffer> entries = log.getEntries(7, 9);
    assertEquals(entries.size(), 3);
    assertBytesEqual(entries.get(0), "7");
    assertBytesEqual(entries.get(1), "8");
    assertBytesEqual(entries.get(2), "9");
  }

  public void testContainsIndex() {
    assertFalse(log.containsIndex(0));
    assertFalse(log.containsIndex(1));
    appendEntries(2);
    assertTrue(log.containsIndex(1));
    assertTrue(log.containsIndex(2));
  }

  public void testIsEmpty() {
    assertTrue(log.isEmpty());
    appendEntries(1);
    assertFalse(log.isEmpty());
  }

  public void testRemoveAfter() {
    appendEntries(4);
    assertTrue(log.containsIndex(3));
    assertTrue(log.containsIndex(4));

    log.removeAfter(2);
    assertEquals(log.firstIndex().longValue(), 1);
    assertEquals(log.lastIndex().longValue(), 2);
    assertFalse(log.containsIndex(3));
    assertFalse(log.containsIndex(4));

    log.removeAfter(0);
    assertFalse(log.containsIndex(1));
    assertNull(log.firstIndex());
    assertNull(log.lastIndex());
    assertEquals(log.size(), 0);
    assertTrue(log.isEmpty());
  }

  /**
   * Tests replacing entries at the end of the log.
   */
  public void testRemoveReplaceEntriess() {
    appendEntries(5);
    log.removeAfter(3);
    assertTrue(log.containsIndex(1));
    assertTrue(log.containsIndex(3));
    assertBytesEqual(log.getEntry(3), 3);
    assertFalse(log.containsIndex(4));
    assertFalse(log.containsIndex(5));

    // Append a few more entries to span segments
    log.appendEntry(Bytes.of(6));
    log.appendEntry(Bytes.of(7));
    log.appendEntry(Bytes.of(8));
    log.appendEntry(Bytes.of(9));
    log.appendEntry(Bytes.of(10));
    assertBytesEqual(log.getEntry(4), 6);
    assertBytesEqual(log.getEntry(8), 10);
  }

  public void shouldDeleteAfterIndex0() {
    log.appendEntry(Bytes.of("foo"));
    log.removeAfter(0);
    assertTrue(log.isEmpty());
    assertEquals(log.size(), 0);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void appendEntryShouldThrowWhenClosed() throws Exception {
    log.close();
    log.appendEntry(Bytes.of("1"));
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void segmentShouldThrowOnEmptyLog() throws Exception {
    log.delete();
    log.segment(10);
  }

  public void testClose() throws Exception {
    appendEntries(5);
    assertTrue(log.isOpen());
    log.close();
    assertFalse(log.isOpen());
  }

  public void testCompactEndOfLog() throws Exception {
    appendEntries(5);
    log.compact(5, Bytes.of(6));
    assertEquals(log.entries(), 1);
    assertEquals(log.size(), log.entries() * entrySize());

    assertEquals(log.firstIndex().longValue(), 5);
    assertEquals(log.lastIndex().longValue(), 5);
    assertBytesEqual(log.getEntry(5), 6);
  }

  public void testCompactMiddleOfLog() throws Exception {
    appendEntries(5);
    log.compact(3, Bytes.of(6));
    assertEquals(log.entries(), 3);
    assertEquals(log.size(), log.entries() * entrySize());

    assertEquals(log.firstIndex().longValue(), 3);
    assertEquals(log.lastIndex().longValue(), 5);
    assertBytesEqual(log.getEntry(3), 6);
    assertBytesEqual(log.getEntry(4), 4);
    assertBytesEqual(log.getEntry(5), 5);
  }

  public void testFirstIndex() throws Exception {
    appendEntries(5);
    assertEquals(log.firstIndex().byteValue(), 1);
  }

  public void testGetEntries() throws Exception {
    appendEntries(5);
    assertEquals(log.getEntries(1, 5).size(), 5);
    assertEquals(log.getEntries(2, 4).size(), 3);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void shouldThrowOnGetEntriesWithOutOfBoundsIndex() throws Exception {
    appendEntries(5);
    log.getEntries(-1, 10);
  }

  public void testGetEntry() throws Exception {
    appendEntries(5);
    assertBytesEqual(log.getEntry(1), 1);
    assertBytesEqual(log.getEntry(2), 2);
    assertBytesEqual(log.getEntry(3), 3);
  }

  public void testIsOpen() throws Throwable {
    assertTrue(log.isOpen());
    log.close();
    assertFalse(log.isOpen());
  }

  public void testSegments() {
    assertEquals(log.segments().size(), 1);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void shouldThrowOnIsOpenAlready() throws Throwable {
    log.open();
  }

  public void testLastIndex() throws Exception {
    appendEntries(5);
    assertEquals(log.lastIndex().longValue(), 5);
  }

  /**
   * Tests calculation of the log size.
   */
  public void testSize() {
    assertFalse(log.containsIndex(0));
    assertFalse(log.containsIndex(1));

    appendEntries(100);
    assertEquals(log.segments().size(), (int) Math.ceil(100.0 / entriesPerSegment));
    assertEquals(log.size(), 100 * entrySize());
    assertFalse(log.isEmpty());
    assertFalse(log.containsIndex(0));
    assertTrue(log.containsIndex(1));

    appendEntries(100);
    assertEquals(log.segments().size(), (int) Math.ceil(200.0 / entriesPerSegment));
    assertEquals(log.size(), 200 * entrySize());
    assertFalse(log.isEmpty());
    assertFalse(log.containsIndex(0));
    assertTrue(log.containsIndex(1));
    assertTrue(log.containsIndex(200));
    assertFalse(log.containsIndex(201));

    assertEquals(log.segments().iterator().next().segment(), 1);
    appendEntries(1);
    assertTrue(log.containsIndex(201));
    assertEquals(log.lastIndex().longValue(), 201);
    assertEquals(log.segment().lastIndex().longValue(), 201);
  }

  public void testEntries() {
    assertEquals(log.entries(), 0);
    appendEntries(10);
    assertEquals(log.entries(), 10);
    log.removeAfter(5);
    assertEquals(log.entries(), 5);
    log.removeAfter(0);
    assertEquals(log.entries(), 0);
  }

  /**
   * Tests that the log rotates segments once the segment size has been reached.
   */
  public void testRotateSegments() {
    assertEquals(log.segments().size(), 1);
    appendEntries(100);
    assertEquals(log.segments().size(), (int) Math.ceil(100.0 / entriesPerSegment));
    assertEquals(log.firstIndex().longValue(), 1);
    assertEquals(log.lastIndex().longValue(), 100);
  }

  /**
   * Tests log segmenting.
   */
  public void testLogSegments() {
    assertTrue(log.isEmpty());
    appendEntries(100);
    assertTrue(log.segments().size() > 1);
    assertFalse(log.isEmpty());
    log.appendEntry(Bytes.of("foo"));
    assertTrue(log.segments().size() > 1);
  }

  /**
   * Appends entries to the log.
   */
  protected void appendEntries(int numEntries) {
    for (int i = 1; i <= numEntries; i++) {
      log.appendEntry(ByteBuffer.allocate(4).putInt(i));
    }
  }

  protected static void assertBytesEqual(ByteBuffer b1, int number) {
    assertEquals(new String(b1.array()), new String(ByteBuffer.allocate(4).putInt(number).array()));
  }

  protected static void assertBytesEqual(ByteBuffer b1, String string) {
    assertEquals(new String(b1.array()), string);
  }
}
