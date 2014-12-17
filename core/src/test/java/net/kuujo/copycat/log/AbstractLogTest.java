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

import static org.testng.Assert.*;

/**
 * Tests log implementations.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 * @author Jonathan Halterman
 */
@Test
public abstract class AbstractLogTest {
  protected Log log;
  protected int segmentSize = 100;
  protected int entriesPerSegment = segmentSize / entrySize();

  /**
   * Creates a test log instance.
   */
  protected abstract Log createLog() throws Throwable;

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

//  public void testAppendEntries() throws Exception {
//    assertEquals(log.appendEntries(Arrays.asList(Bytes.of("1"), Bytes.of("2"), Bytes.of("3"))),
//      Arrays.asList(1L, 2L, 3L));
//  }
//
//  public void testAppendEntry() throws Exception {
//    assertEquals(log.appendEntry(Bytes.of("1")), 1L);
//    assertEquals(log.appendEntry(Bytes.of("2")), 2L);
//    assertEquals(log.appendEntry(Bytes.of("3")), 3L);
//  }
//
//  /**
//   * Tests appending and getting entries.
//   */
//  public void testAppendGetEntries() {
//    appendEntries(5);
//    assertFalse(log.isEmpty());
//    assertFalse(log.containsIndex(0));
//    assertTrue(log.containsIndex(1));
//    assertBytesEqual(log.getEntry(1), "1");
//    assertBytesEqual(log.getEntry(2), "2");
//    assertBytesEqual(log.getEntry(3), "3");
//    assertBytesEqual(log.getEntry(4), "4");
//    assertBytesEqual(log.getEntry(5), "5");
//    assertFalse(log.containsIndex(6));
//    log.appendEntry(Bytes.of("6"));
//    log.appendEntry(Bytes.of("7"));
//    log.appendEntry(Bytes.of("8"));
//    log.appendEntry(Bytes.of("9"));
//    log.appendEntry(Bytes.of("10"));
//    assertTrue(log.containsIndex(10));
//    assertFalse(log.containsIndex(11));
//    List<ByteBuffer> entries = log.getEntries(7, 9);
//    assertEquals(entries.size(), 3);
//    assertBytesEqual(entries.get(0), "7");
//    assertBytesEqual(entries.get(1), "8");
//    assertBytesEqual(entries.get(2), "9");
//  }
//
//  /**
//   * Tests replacing entries at the end of the log.
//   */
//  public void testRemoveReplaceEntries() {
//    appendEntries(5);
//    assertTrue(log.containsIndex(1));
//    assertTrue(log.containsIndex(5));
//    log.removeAfter(3);
//    assertTrue(log.containsIndex(1));
//    assertTrue(log.containsIndex(3));
//    assertBytesEqual(log.getEntry(3), "3");
//    assertFalse(log.containsIndex(4));
//    assertFalse(log.containsIndex(5));
//    log.appendEntry(Bytes.of("6"));
//    log.appendEntry(Bytes.of("7"));
//    log.appendEntry(Bytes.of("8"));
//    log.appendEntry(Bytes.of("9"));
//    log.appendEntry(Bytes.of("10"));
//    assertTrue(log.containsIndex(8));
//    assertBytesEqual(log.getEntry(4), "6");
//    assertBytesEqual(log.getEntry(8), "10");
//  }
//
//  public void shouldDeleteAfterIndex0() {
//    log.appendEntry(ByteBuffer.wrap("1".getBytes()));
//    log.removeAfter(0);
//    assertTrue(log.isEmpty());
//    assertEquals(log.size(), 0);
//  }
//
//  @Test(expectedExceptions = IllegalStateException.class)
//  public void testThrowsIllegalStateExceptionWhenClosed() throws Exception {
//    log.close();
//    log.appendEntry(Bytes.of("1"));
//  }
//
//  public void testClose() throws Exception {
//    appendEntries(5);
//    assertTrue(log.isOpen());
//    log.close();
//    assertFalse(log.isOpen());
//  }
//
//  public void testCompactEndOfLog() throws Exception {
//    appendEntries(5);
//    log.compact(5, Bytes.of("foo"));
//    assertEquals(log.size(), 1);
//
//    assertEquals(log.firstIndex(), 5);
//    assertEquals(log.lastIndex(), 5);
//    assertBytesEqual(log.getEntry(5), "foo");
//  }
//
//  public void testCompactMiddleOfLog() throws Exception {
//    appendEntries(5);
//    log.compact(3, Bytes.of("foo"));
//    assertEquals(log.size(), 3);
//
//    assertEquals(log.firstIndex(), 3);
//    assertEquals(log.lastIndex(), 5);
//    assertBytesEqual(log.getEntry(3), "3");
//    assertBytesEqual(log.getEntry(4), "4");
//    assertBytesEqual(log.getEntry(5), "5");
//  }
//
//  public void testContainsIndex() throws Exception {
//    appendEntries(5);
//
//    assertTrue(log.containsIndex(3));
//    assertFalse(log.containsIndex(7));
//  }
//
//  public void testFirstIndex() throws Exception {
//    appendEntries(5);
//    assertEquals(log.firstIndex(), 1);
//  }
//
//  public void testGetEntries() throws Exception {
//    appendEntries(5);
//    assertEquals(log.getEntries(1, 5).size(), 5);
//    assertEquals(log.getEntries(2, 4).size(), 3);
//  }
//
//  @Test(expectedExceptions = IndexOutOfBoundsException.class)
//  public void shouldThrowOnGetEntriesWithOutOfBoundsIndex() throws Exception {
//    appendEntries(5);
//    log.getEntries(-1, 10);
//  }
//
//  public void testGetEntry() throws Exception {
//    appendEntries(5);
//    assertBytesEqual(log.getEntry(1), "1");
//    assertBytesEqual(log.getEntry(2), "2");
//    assertBytesEqual(log.getEntry(3), "3");
//  }
//
//  public void testIsEmpty() {
//    assertTrue(log.isEmpty());
//    assertEquals(log.appendEntry(Bytes.of("foo")), 1);
//    assertFalse(log.isEmpty());
//  }
//
//  public void testIsOpen() throws Throwable {
//    assertTrue(log.isOpen());
//    log.close();
//    assertFalse(log.isOpen());
//  }
//
//  public void testSegments() {
//    assertEquals(log.segments().size(), 1);
//  }
//
//  @Test(expectedExceptions = IllegalStateException.class)
//  public void shouldThrowOnIsOpenAlready() throws Throwable {
//    log.open();
//  }
//
//  public void testLastIndex() throws Exception {
//    appendEntries(5);
//    assertEquals(log.lastIndex(), 5);
//  }
//
//  public void testRemoveAfter() throws Exception {
//    appendEntries(5);
//    log.removeAfter(2);
//
//    assertBytesEqual(log.getEntry(log.firstIndex()), "1");
//    assertBytesEqual(log.getEntry(log.lastIndex()), "2");
//  }

  /**
   * Tests calculation of the log size.
   */
  public void testSize() {
    assertFalse(log.containsIndex(0));
    assertFalse(log.containsIndex(1));

    appendEntries(150);
    assertEquals(log.segments().size(), 150 / entriesPerSegment);
    assertEquals(log.size(), 150 * entrySize());
    assertFalse(log.isEmpty());
    assertFalse(log.containsIndex(0));
    assertTrue(log.containsIndex(1));

    appendEntries(150);
    assertEquals(log.segments().size(), 300 / entriesPerSegment);
    assertEquals(log.size(), 300 * entrySize());
    assertFalse(log.isEmpty());
    assertFalse(log.containsIndex(0));
    assertTrue(log.containsIndex(1));
    assertTrue(log.containsIndex(300));
    assertFalse(log.containsIndex(301));

    assertEquals(log.segments().iterator().next().segment(), 1);
    assertEquals(log.segment().segment(), 300);
  }

//  /**
//   * Tests that the log rotates segments once the segment size has been reached.
//   */
//  public void testRotateSegments() {
//    assertEquals(log.segments().size(), 1);
//    appendEntries(1000);
//    assertEquals(log.segments().size(), 10);
//    assertEquals(log.firstIndex(), 1);
//    assertEquals(log.lastIndex(), 1000);
//  }
//
//  /**
//   * Tests log segmenting.
//   */
//  public void testLogSegments() {
//    assertTrue(log.isEmpty());
//    appendEntries(1000);
//    assertTrue(log.segments().size() > 1);
//    assertFalse(log.isEmpty());
//    log.appendEntry(Bytes.of("foo"));
//    assertTrue(log.segments().size() > 1);
//  }
//
  /**
   * Appends entries to the log.
   */
  protected void appendEntries(int numEntries) {
    for (int i = 1; i <= numEntries; i++) {
      log.appendEntry(Bytes.of(String.valueOf(i)));
    }
  }

//
//  protected static void assertBytesEqual(ByteBuffer b1, String string) {
//    assertEquals(b1.array(), string.getBytes());
//  }
}
