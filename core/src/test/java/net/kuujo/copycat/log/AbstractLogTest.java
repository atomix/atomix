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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import net.kuujo.copycat.internal.util.Bytes;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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

  /**
   * Asserts that entries spanning 3 segments are appended with the expected indexes.
   */
  public void testAppendEntry() throws Exception {
    for (int i = 1; i <= entriesPerSegment * 3; i++)
      assertEquals(log.appendEntry(Bytes.of(i)), i);
  }

  /**
   * Asserts that entries spanning 3 segments are appended with the expected indexes.
   */
  public void testAppendEntries() throws Exception {
    List<Long> expectedIndexes = IntStream.range(1, entriesPerSegment * 3 + 1)
      .asLongStream()
      .boxed()
      .collect(Collectors.toList());
    assertEquals(appendEntries(entriesPerSegment * 3), expectedIndexes);
  }

  /**
   * Asserts that appending and getting entries works as expected across segments.
   */
  public void testAppendGetEntries() {
    // Append 3 segments
    List<Long> indexes = appendEntries(entriesPerSegment * 3);
    assertFalse(log.isEmpty());
    assertFalse(log.containsIndex(0));

    // Assert that entries can be retrieved
    indexes.stream().forEach(i -> assertBytesEqual(log.getEntry(i), i));
    assertFalse(log.containsIndex(indexes.size() + 1));

    // Append 2 more segments
    List<Long> moreIndexes = appendEntries(entriesPerSegment * 2);
    moreIndexes.stream().forEach(i -> assertBytesEqual(log.getEntry(i), i));
    assertFalse(log.containsIndex(indexes.size() + moreIndexes.size() + 1));

    // Fetch 3 segments worth of entries, spanning 4 segments
    List<ByteBuffer> entries = log.getEntries(3, entriesPerSegment * 3 + 2);
    assertEquals(entries.size(), entriesPerSegment * 3);
    for (int i = 0; i < entries.size(); i++)
      assertBytesEqual(entries.get(i), i + 3);
  }

  /**
   * Asserts that containsIndex works as expected across segments.
   */
  public void testContainsIndex() {
    assertFalse(log.containsIndex(0));
    assertFalse(log.containsIndex(1));

    appendEntries(entriesPerSegment * 3);
    for (int i = 1; i <= entriesPerSegment * 3; i++)
      assertTrue(log.containsIndex(i));
    assertFalse(log.containsIndex(entriesPerSegment * 3 + 1));
  }

  public void testIsEmpty() {
    assertTrue(log.isEmpty());
    appendEntries(1);
    assertFalse(log.isEmpty());
  }

  /**
   * Asserts that removeAfter works as expected across segments.
   */
  public void testRemoveAfter() {
    appendEntries(entriesPerSegment * 3);

    // Remove last 2 segments
    log.removeAfter(entriesPerSegment + 2);
    assertEquals(log.firstIndex().longValue(), 1);
    assertEquals(log.lastIndex().longValue(), entriesPerSegment + 2);
    assertEquals(log.entries(), entriesPerSegment + 2);
    assertEquals(log.segments().size(), 2);
    assertFalse(log.containsIndex(entriesPerSegment * 3));

    // Remove remaining segment
    log.removeAfter(0);
    assertFalse(log.containsIndex(1));
    assertNull(log.firstIndex());
    assertNull(log.lastIndex());
    assertEquals(log.size(), 0);
    assertTrue(log.isEmpty());
    assertEquals(log.entries(), 0);
    assertEquals(log.segments().size(), 1);
  }

  /**
   * Tests replacing entries at the end of the log across segments.
   */
  public void testRemoveAndReplaceEntries() {
    appendEntries(entriesPerSegment * 3);

    // Remove last 2 segments
    log.removeAfter(entriesPerSegment + 2);

    // Append 3 more segments
    int nextEntryId = 5000;
    List<Long> moreIndexes = appendEntries(entriesPerSegment * 3, nextEntryId);
    for (int i = 0; i < moreIndexes.size(); i++)
      assertBytesEqual(log.getEntry(moreIndexes.get(i)), nextEntryId + i);
    assertFalse(log.containsIndex((entriesPerSegment + 2) + (entriesPerSegment * 3) + 1));

    // Remove all segments
    log.removeAfter(0);
    appendEntries(entriesPerSegment * 2);
    assertEquals(log.firstIndex().longValue(), 1);
    assertEquals(log.lastIndex().longValue(), entriesPerSegment * 2);
    assertEquals(log.entries(), entriesPerSegment * 2);
    assertEquals(log.segments().size(), 3);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testRemoveAfterNegativeIndex() {
    log.removeAfter(-1);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testRemoveAfterHighIndex() {
    appendEntries(3);
    log.removeAfter(3);
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

  /**
   * Tests {@link AbstractLog#close()}
   */
  public void testClose() throws Exception {
    appendEntries(5);
    assertTrue(log.isOpen());
    log.close();
    assertFalse(log.isOpen());
  }

  /**
   * Tests {@link AbstractLog#compact(long, ByteBuffer)} on the log head.
   */
  public void testCompactLogHead() {
    appendEntries(entriesPerSegment * 3);

    log.compact(1, Bytes.of(5000));
    assertBytesEqual(log.getEntry(log.firstIndex()), 5000);
    assertEquals(log.entries(), entriesPerSegment * 3);
    assertEquals(log.segments().size(), 4);
    assertEquals(log.firstIndex().longValue(), 1);
    assertEquals(log.lastIndex().longValue(), entriesPerSegment * 3);
  }

  /**
   * Tests {@link AbstractLog#compact(long, ByteBuffer)} on the log tail.
   */
  public void testCompactLogTail() {
    appendEntries(entriesPerSegment * 3);
    log.compact(entriesPerSegment * 3, Bytes.of(5000));

    assertBytesEqual(log.getEntry(entriesPerSegment * 3), 5000);
    assertEquals(log.entries(), 1);
    assertEquals(log.size(), entrySize());
    assertEquals(log.segments().size(), 1);
    assertEquals(log.firstIndex().longValue(), entriesPerSegment * 3);
    assertEquals(log.lastIndex().longValue(), entriesPerSegment * 3);
  }

  /**
   * Tests {@link AbstractLog#compact(long, ByteBuffer)} on the log middle.
   */
  public void testCompactLogMiddle() {
    appendEntries(entriesPerSegment * 3);
    log.compact(entriesPerSegment + 3, Bytes.of(5000));

    assertBytesEqual(log.getEntry(entriesPerSegment + 3), 5000);
    int expectedEntries = (entriesPerSegment * 3) - (entriesPerSegment + 3) + 1;
    assertEquals(log.entries(), expectedEntries);
    assertEquals(log.size(), entrySize() * expectedEntries);
    assertEquals(log.firstIndex().longValue(), entriesPerSegment + 3);
    assertEquals(log.lastIndex().longValue(), entriesPerSegment * 3);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testCompactNegativeIndex() {
    appendEntries(3);
    log.compact(-2, Bytes.of(5000));
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testCompactHighIndex() {
    appendEntries(entriesPerSegment + 3);
    log.compact(entriesPerSegment * 5, Bytes.of(5000));
  }

  /**
   * Tests {@link AbstractLog#firstIndex()} across segments.
   */
  public void testFirstIndex() {
    appendEntries(entriesPerSegment * 3);
    assertEquals(log.firstIndex().byteValue(), 1);
  }

  /**
   * Tests {@link AbstractLog#getEntry(long)} across segments.
   */
  public void testGetEntry() {
    appendEntries(entriesPerSegment * 3);
    for (int i = 1; i <= entriesPerSegment * 3; i++)
      assertBytesEqual(log.getEntry(i), i);
  }

  /**
   * Tests {@link AbstractLog#getEntries(long, long)} across segments.
   */
  public void testGetEntries() {
    appendEntries(entriesPerSegment * 3);
    assertEquals(log.getEntries(1, 5).size(), 5);
    assertEquals(log.getEntries(2, 4).size(), 3);
  }

  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void shouldThrowOnGetEntriesWithOutOfBoundsIndex() throws Exception {
    appendEntries(5);
    log.getEntries(-1, 10);
  }

  /**
   * Tests {@link AbstractLog#isOpen()}.
   */
  public void testIsOpen() throws Throwable {
    assertTrue(log.isOpen());
    log.close();
    assertFalse(log.isOpen());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void shouldThrowOnIsOpenAlready() throws Throwable {
    log.open();
  }

  /**
   * Tests {@link AbstractLog#lastIndex()} across segments.
   */
  public void testLastIndex() {
    appendEntries(entriesPerSegment * 3);
    assertEquals(log.lastIndex().longValue(), entriesPerSegment * 3);
  }

  /**
   * Tests {@link AbstractLog#size()} across segments.
   */
  public void testSize() {
    assertEquals(log.size(), 0);

    appendEntries(entriesPerSegment * 3);
    assertEquals(log.segments().size(), 4);
    assertEquals(log.size(), entrySize() * entriesPerSegment * 3);
    assertFalse(log.isEmpty());

    appendEntries(entriesPerSegment * 2);
    assertEquals(log.segments().size(), 6);
    assertEquals(log.size(), entrySize() * entriesPerSegment * 5);

    log.removeAfter(entriesPerSegment * 2 + 1);
    assertEquals(log.segments().size(), 3);
    assertEquals(log.size(), entrySize() * (entriesPerSegment * 2 + 1));
  }

  /**
   * Test {@link AbstractLog#entries()} across segments.
   */
  public void testEntries() {
    assertEquals(log.entries(), 0);
    appendEntries(entriesPerSegment * 3);
    assertEquals(log.entries(), entriesPerSegment * 3);
    log.removeAfter(entriesPerSegment * 2);
    assertEquals(log.entries(), entriesPerSegment * 2);
    log.removeAfter(0);
    assertEquals(log.entries(), 0);
  }

  /**
   * Tests {@link AbstractLog#size()} across segments.
   */
  public void testSegments() {
    assertEquals(log.segments().size(), 1);
    appendEntries(entriesPerSegment * 3);
    assertEquals(log.segments().size(), 4);
    assertEquals(log.lastIndex().longValue(), entriesPerSegment * 3);
  }

  /**
   * Appends {@code numEntries} increasingly numbered ByteBuffer wrapped entries to the log.
   */
  protected List<Long> appendEntries(int numEntries) {
    return appendEntries(numEntries, (int) log.entries() + 1);
  }

  /**
   * Appends {@code numEntries} increasingly numbered ByteBuffer wrapped entries to the log,
   * starting at the {@code startingId}.
   */
  protected List<Long> appendEntries(int numEntries, int startingId) {
    List<Integer> entryIds = IntStream.range(startingId, startingId + numEntries).boxed().collect(Collectors.toList());
    return entryIds.stream().map(e -> log.appendEntry(ByteBuffer.allocate(4).putInt(e))).collect(Collectors.toList());
  }

  protected static void assertBytesEqual(ByteBuffer b1, long number) {
    assertEquals(new BigInteger(b1.array()), new BigInteger(ByteBuffer.allocate(4).putInt((int) number).array()));
  }

  protected static void assertBytesEqual(ByteBuffer b1, int number) {
    assertEquals(new BigInteger(b1.array()), new BigInteger(ByteBuffer.allocate(4).putInt(number).array()));
  }

  protected static void assertBytesEqual(ByteBuffer b1, String string) {
    assertEquals(new String(b1.array()), string);
  }
}
