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
package net.kuujo.copycat.test.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.NoOpEntry;

import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class LogTest {

  /**
   * Creates a test log instance.
   */
  protected abstract Log createLog();

  @Test
  public void testAppendEntry() {
    Log log = createLog();
    log.open();
    long index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    log.removeAfter(0);
  }

  @Test
  public void testContainsEntry() {
    Log log = createLog();
    log.open();
    long index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    assertTrue(log.containsEntry(1));
    log.removeAfter(0);
  }

  @Test
  public void testGetEntry() {
    Log log = createLog();
    log.open();
    long index = log.appendEntry(new NoOpEntry());
    Entry entry = log.getEntry(index);
    assertTrue(entry instanceof NoOpEntry);
    log.removeAfter(0);
  }

  @Test
  public void testSetEntry() {
    Log log = createLog();
    log.open();
    long index1 = log.appendEntry(new NoOpEntry(1));
    long index2 = log.appendEntry(new NoOpEntry(2));
    long index3 = log.appendEntry(new NoOpEntry(3));
    Entry entry1 = log.getEntry(index1);
    assertTrue(entry1 instanceof NoOpEntry);
    assertEquals(1, entry1.term());
    Entry entry2 = log.getEntry(index2);
    assertTrue(entry2 instanceof NoOpEntry);
    assertEquals(2, entry2.term());
    Entry entry3 = log.getEntry(index3);
    assertTrue(entry3 instanceof NoOpEntry);
    assertEquals(3, entry3.term());
    log.setEntry(index2, new NoOpEntry(4));
    Entry entry4 = log.getEntry(index2);
    assertTrue(entry4 instanceof NoOpEntry);
    assertEquals(4, entry4.term());
    log.removeAfter(0);
  }

  @Test
  public void testFirstIndex() {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry());
    assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 3);
    assertTrue(log.firstIndex() == 1);
    assertTrue(log.lastIndex() == 3);
    log.removeAfter(0);
  }

  @Test
  public void testFirstEntry() {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry());
    assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 3);
    Entry entry = log.firstEntry();
    assertTrue(entry instanceof NoOpEntry);
    log.removeAfter(0);
  }

  @Test
  public void testLastIndex() {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry());
    assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 3);
    assertTrue(log.lastIndex() == 3);
    log.removeAfter(0);
  }

  @Test
  public void testLastEntry() {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry());
    assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 3);
    Entry entry = log.lastEntry();
    assertTrue(entry instanceof CommandEntry);
    log.removeAfter(0);
  }

  @Test
  public void testRemoveBefore() {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry());
    assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 3);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 4);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 5);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 6);

    log.removeBefore(3);
    assertTrue(log.firstIndex() == 3);
    assertTrue(log.firstEntry() instanceof CommandEntry);
    log.removeAfter(0);
  }

  @Test
  public void testRemoveAfter() {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry());
    assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 3);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 4);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 5);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 6);

    log.removeAfter(2);
    assertTrue(log.firstIndex() == 1);
    assertTrue(log.lastIndex() == 2);
    assertTrue(log.firstEntry() instanceof NoOpEntry);
    assertTrue(log.lastEntry() instanceof ConfigurationEntry);
    log.removeAfter(0);
  }

  @Test
  public void testFullHandler() {
    Log log = createLog();
    log.open();
    log.setMaxSize(10);
    final AtomicBoolean complete = new AtomicBoolean(false);
    log.fullHandler(new Handler<Void>() {
      @Override
      public void handle(Void event) {
        complete.set(true);
      }
    });
    log.appendEntry(new NoOpEntry(0));
    log.appendEntry(new NoOpEntry(1));
    log.appendEntry(new NoOpEntry(2));
    log.appendEntry(new NoOpEntry(3));
    log.appendEntry(new NoOpEntry(4));
    log.appendEntry(new NoOpEntry(5));
    log.appendEntry(new NoOpEntry(6));
    log.appendEntry(new NoOpEntry(7));
    log.appendEntry(new NoOpEntry(8));
    log.appendEntry(new NoOpEntry(9));
    assertTrue(complete.get());
  }

  @Test
  public void testDrainHandler() {
    Log log = createLog();
    log.open();
    log.setMaxSize(10);
    final AtomicBoolean complete = new AtomicBoolean(false);
    log.drainHandler(new Handler<Void>() {
      @Override
      public void handle(Void event) {
        complete.set(true);
      }
    });
    log.appendEntry(new NoOpEntry(0));
    log.appendEntry(new NoOpEntry(1));
    log.appendEntry(new NoOpEntry(2));
    log.appendEntry(new NoOpEntry(3));
    log.appendEntry(new NoOpEntry(4));
    log.appendEntry(new NoOpEntry(5));
    log.appendEntry(new NoOpEntry(6));
    log.appendEntry(new NoOpEntry(7));
    log.appendEntry(new NoOpEntry(8));
    log.appendEntry(new NoOpEntry(9));
    assertFalse(complete.get());
    log.removeAfter(0);
    assertTrue(complete.get());
  }

}
