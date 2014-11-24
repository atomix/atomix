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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.OperationEntry;
import net.kuujo.copycat.internal.log.SnapshotEntry;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests log implementations.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public abstract class AbstractLogTest {
  protected Log log;

  /**
   * Creates a test log instance.
   */
  protected abstract Log createLog() throws Throwable;

  /**
   * Deletes the test log instance.
   */
  protected void deleteLog() throws Throwable {
  }

  @BeforeMethod
  protected void beforeMethod() throws Throwable {
    log = createLog();
    log.open();
  }

  @AfterMethod
  protected void afterMethod() throws Throwable {
    try {
      log.close();
    } catch (Exception ignore) {
    } finally {
      log.delete();
    }
  }

  public void testAppendEntries() throws Exception {
    Entry entry = new OperationEntry(1, "foo", "bar");
    assertEquals(log.appendEntries(entry, entry, entry), Arrays.asList(1L, 2L, 3L));
  }

  public void testAppendEntry() throws Exception {
    assertEquals(log.appendEntry(new OperationEntry(1, "foo", "bar")), 1);
    assertEquals(log.appendEntry(new OperationEntry(1, "foo", "bar")), 2);
    assertEquals(log.appendEntry(new OperationEntry(1, "foo", "bar")), 3);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testThrowsIllegalStateExceptionWhenClosed() throws Exception {
    log.close();
    log.appendEntry(new OperationEntry(1, "foo", "bar"));
  }

  public void testClose() throws Exception {
    appendEntries();
    assertTrue(log.isOpen());
    log.close();
    assertFalse(log.isOpen());
  }

  public void testCompactEndOfLog() throws Exception {
    appendEntries();
    log.compact(5,
      new SnapshotEntry(1, new Cluster("foo", "bar", "baz"), "Hello world!".getBytes()));

    assertEquals(log.firstIndex(), 5);
    assertEquals(log.lastIndex(), 5);
    SnapshotEntry entry = log.getEntry(5);
    assertEquals(entry.term(), 1);
    assertEquals("Hello world!", new String(entry.data()));
  }

  public void testCompactMiddleOfLog() throws Exception {
    appendEntries();
    log.compact(3,
      new SnapshotEntry(1, new Cluster("foo", "bar", "baz"), "Hello world!".getBytes()));

    assertEquals(log.firstIndex(), 3);
    assertEquals(log.lastIndex(), 5);
    SnapshotEntry entry = log.getEntry(3);
    assertEquals(entry.term(), 1);
    assertEquals("Hello world!", new String(entry.data()));
    OperationEntry entry2 = log.getEntry(4);
    assertEquals(entry2.term(), 1);
    assertEquals("bar", entry2.operation());
    OperationEntry entry3 = log.getEntry(5);
    assertEquals(entry3.term(), 1);
    assertEquals("baz", entry3.operation());
  }

  public void testContainsEntry() throws Exception {
    appendEntries();

    assertTrue(log.containsEntry(3));
    assertFalse(log.containsEntry(7));
  }

  public void testFirstEntry() throws Exception {
    appendEntries();
    assertTrue(log.firstEntry() instanceof SnapshotEntry);
  }

  public void testFirstIndex() throws Exception {
    appendEntries();
    assertEquals(log.firstIndex(), 1);
  }

  public void testGetEntries() throws Exception {
    appendEntries();
    assertEquals(log.getEntries(1, 5).size(), 5);
    assertEquals(log.getEntries(2, 4).size(), 3);
  }

  @Test(expectedExceptions = LogIndexOutOfBoundsException.class)
  public void shouldThrowOnGetEntriesWithOutOfBoundsIndex() throws Exception {
    appendEntries();
    log.getEntries(-1, 10);
  }

  public void testGetEntry() throws Exception {
    appendEntries();
    assertTrue(log.getEntry(1) instanceof SnapshotEntry);
    assertTrue(log.getEntry(2) instanceof ConfigurationEntry);
    assertTrue(log.getEntry(3) instanceof OperationEntry);
  }

  public void testIsEmpty() {
    assertTrue(log.isEmpty());
    assertEquals(log.appendEntry(new OperationEntry(1, "foo", "bar")), 1);
    assertFalse(log.isEmpty());
  }

  public void testIsOpen() throws Throwable {
    assertTrue(log.isOpen());
    log.close();
    assertFalse(log.isOpen());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void shouldThrowOnIsOpenAlready() throws Throwable {
    log.open();
  }

  public void testLastEntry() throws Exception {
    appendEntries();
    assertTrue(log.lastEntry() instanceof OperationEntry);
  }

  public void testLastIndex() throws Exception {
    appendEntries();
    assertEquals(log.lastIndex(), 5);
  }

  public void testRemoveAfter() throws Exception {
    appendEntries();
    log.removeAfter(2);

    assertEquals(log.firstIndex(), 1);
    assertTrue(log.firstEntry() instanceof SnapshotEntry);
    assertEquals(log.lastIndex(), 2);
    assertTrue(log.lastEntry() instanceof ConfigurationEntry);
  }

  public void testSize() {
    long size = log.size();
    log.appendEntry(new OperationEntry(1, "foo", "bar"));
    assertNotEquals(size, size = log.size());
    log.appendEntry(new OperationEntry(1, "foo", "bar"));
    assertNotEquals(size, size = log.size());
    log.appendEntry(new OperationEntry(1, "foo", "bar"));
    assertNotEquals(size, size = log.size());
  }

  private void appendEntries() {
    log.appendEntry(new SnapshotEntry(1, new Cluster("foo", "bar", "baz"), new byte[10]));
    log.appendEntry(new ConfigurationEntry(1, new Cluster("foo", "bar", "baz")));
    log.appendEntry(new OperationEntry(1, "foo", Arrays.asList("bar", "baz")));
    log.appendEntry(new OperationEntry(1, "bar", Arrays.asList("bar", "baz")));
    log.appendEntry(new OperationEntry(1, "baz", Arrays.asList("bar", "baz")));
  }
}
