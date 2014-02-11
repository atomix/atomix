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

import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.MemoryLog;
import net.kuujo.copycat.log.NoOpEntry;

import org.junit.Test;
import org.vertx.java.core.json.JsonObject;

import static org.junit.Assert.assertTrue;

/**
 * In-memory log tests.
 *
 * @author Jordan Halterman
 */
public class MemoryLogTest {

  @Test
  public void testAppendEntry() {
    Log log = new MemoryLog();
    log.open("test");
    long index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
  }

  @Test
  public void testContainsEntry() {
    Log log = new MemoryLog();
    log.open("test");
    long index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    assertTrue(log.containsEntry(1));
  }

  @Test
  public void testLoadEntry() {
    Log log = new MemoryLog();
    log.open("test");
    long index = log.appendEntry(new NoOpEntry());
    Entry entry = log.getEntry(index);
    assertTrue(entry instanceof NoOpEntry);
  }

  @Test
  public void testFirstIndex() {
    Log log = new MemoryLog();
    log.open("test");
    long index;
    index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry());
    assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 3);
    assertTrue(log.firstIndex() == 1);
    assertTrue(log.lastIndex() == 3);
  }

  @Test
  public void testFirstEntry() {
    Log log = new MemoryLog();
    log.open("test");
    long index;
    index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry());
    assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 3);
    Entry entry = log.firstEntry();
    assertTrue(entry instanceof NoOpEntry);
  }

  @Test
  public void testLastIndex() {
    Log log = new MemoryLog();
    log.open("test");
    long index;
    index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry());
    assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 3);
    assertTrue(log.lastIndex() == 3);
  }

  @Test
  public void testLastEntry() {
    Log log = new MemoryLog();
    log.open("test");
    long index;
    index = log.appendEntry(new NoOpEntry());
    assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry());
    assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new JsonObject()));
    assertTrue(index == 3);
    Entry entry = log.lastEntry();
    assertTrue(entry instanceof CommandEntry);
  }

  @Test
  public void testRemoveBefore() {
    Log log = new MemoryLog();
    log.open("test");
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
  }

  @Test
  public void testRemoveAfter() {
    Log log = new MemoryLog();
    log.open("test");
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
  }

}
