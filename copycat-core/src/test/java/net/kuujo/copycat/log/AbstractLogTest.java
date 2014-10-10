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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.log.CommandEntry;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.NoOpEntry;
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
@SuppressWarnings("unchecked")
public abstract class AbstractLogTest {
  protected Log log;

  /**
   * Creates a test log instance.
   */
  protected abstract Log createLog() throws Throwable;

  /**
   * Deletes the test log instance.
   */
  protected void deleteLog() throws Throwable {}

  @BeforeMethod
  public void beforeMethod() throws Throwable {
    log = createLog();
    log.open();
  }

  @AfterMethod
  public void afterMethod() throws Throwable {
    log.close();
    log.delete();
  }

  public void testAppendEntries() throws Exception {
    Entry entry = new NoOpEntry(1);
    assertEquals(log.appendEntries(entry, entry, entry),
        Arrays.asList(Long.valueOf(1), Long.valueOf(2), Long.valueOf(3)));
  }

  public void testAppendEntry() throws Exception {
    assertEquals(log.appendEntry(new NoOpEntry(1)), 1);
    assertEquals(log.appendEntry(new NoOpEntry(1)), 2);
    assertEquals(log.appendEntry(new NoOpEntry(1)), 3);
  }

  public void testCompactEndOfLog() throws Exception {
    if (log instanceof Compactable) {
      appendEntries();
      ((Compactable) log).compact(5,
          new SnapshotEntry(1, new ClusterConfig().withLocalMember(new Member("foo"))
              .withRemoteMembers(new Member("bar"), new Member("baz")), "Hello world!".getBytes()));

      assertEquals(log.firstIndex(), 5);
      assertEquals(log.lastIndex(), 5);
      SnapshotEntry entry = log.getEntry(5);
      assertEquals(entry.term(), 1);
      assertEquals("Hello world!", new String(entry.data()));
    }
  }

  public void testCompactMiddleOfLog() throws Exception {
    if (log instanceof Compactable) {
      appendEntries();
      ((Compactable) log).compact(3,
          new SnapshotEntry(1, new ClusterConfig().withLocalMember(new Member("foo"))
              .withRemoteMembers(new Member("bar"), new Member("baz")), "Hello world!".getBytes()));

      assertEquals(log.firstIndex(), 3);
      assertEquals(log.lastIndex(), 5);
      SnapshotEntry entry = log.getEntry(3);
      assertEquals(entry.term(), 1);
      assertEquals("Hello world!", new String(entry.data()));
      CommandEntry entry2 = log.getEntry(4);
      assertEquals(entry2.term(), 1);
      assertEquals("bar", entry2.command());
      CommandEntry entry3 = log.getEntry(5);
      assertEquals(entry3.term(), 1);
      assertEquals("baz", entry3.command());
    }
  }

  public void testContainsEntry() throws Exception {
    appendEntries();

    assertTrue(log.containsEntry(3));
    assertFalse(log.containsEntry(7));
  }

  public void testFirstEntry() throws Exception {
    appendEntries();
    assertTrue(log.firstEntry() instanceof NoOpEntry);
  }

  public void testFirstIndex() throws Exception {
    appendEntries();
    assertEquals(log.firstIndex(), 1);
  }

  public void testGetEntry() throws Exception {
    appendEntries();
    assertTrue(log.getEntry(1) instanceof NoOpEntry);
    assertTrue(log.getEntry(2) instanceof ConfigurationEntry);
    assertTrue(log.getEntry(3) instanceof CommandEntry);
  }

  public void testIsEmpty() {
    assertTrue(log.isEmpty());
    assertEquals(log.appendEntry(new NoOpEntry(1)), 1);
    assertFalse(log.isEmpty());
  }

  public void testLastEntry() throws Exception {
    appendEntries();
    assertTrue(log.lastEntry() instanceof CommandEntry);
  }

  public void testLastIndex() throws Exception {
    appendEntries();
    assertEquals(log.lastIndex(), 5);
  }

  public void testRemoveAfter() throws Exception {
    appendEntries();
    log.removeAfter(2);

    assertEquals(log.firstIndex(), 1);
    assertTrue(log.firstEntry() instanceof NoOpEntry);
    assertEquals(log.lastIndex(), 2);
    assertTrue(log.lastEntry() instanceof ConfigurationEntry);
  }

  public void testRemoveEntry() throws Exception {
    appendEntries();

    log.removeEntry(1);
    log.removeEntry(3);
    log.removeEntry(5);

    assertFalse(log.containsEntry(1));
    assertFalse(log.containsEntry(3));
    assertFalse(log.containsEntry(5));
    assertTrue(log.containsEntry(2));
    assertTrue(log.containsEntry(4));
  }

  public void testSize() {
    long size = log.size();
    log.appendEntry(new NoOpEntry(1));
    assertNotEquals(size, size = log.size());
    log.appendEntry(new NoOpEntry(1));
    assertNotEquals(size, size = log.size());
    log.appendEntry(new NoOpEntry(1));
    assertNotEquals(size, size = log.size());
  }

  private void appendEntries() {
    log.appendEntry(new NoOpEntry(1));
    log.appendEntry(new ConfigurationEntry(1, new ClusterConfig()
        .withLocalMember(new Member("foo")).withRemoteMembers(new Member("bar"), new Member("baz"))));
    log.appendEntry(new CommandEntry(1, "foo", Arrays.asList("bar", "baz")));
    log.appendEntry(new CommandEntry(1, "bar", Arrays.asList("bar", "baz")));
    log.appendEntry(new CommandEntry(1, "baz", Arrays.asList("bar", "baz")));
  }
}
