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

import static org.junit.Assert.assertTrue;
import net.kuujo.copycat.log.CommandEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.NoOpEntry;
import net.kuujo.copycat.log.impl.FileLog;
import net.kuujo.copycat.serializer.Serializer;

import org.junit.Test;
import org.vertx.java.core.json.JsonObject;

/**
 * File log tests.
 *
 * @author Jordan Halterman
 */
public class FileLogTest {
  private final Serializer serializer = Serializer.getInstance();

  @Test
  public void testAppendEntry() {
    Log log = new FileLog();
    log.open("test");
    long index = log.appendEntry(serializer.writeString(new NoOpEntry()));
    assertTrue(index == 1);
    log.removeAfter(0);
  }

  @Test
  public void testContainsEntry() {
    Log log = new FileLog();
    log.open("test");
    long index = log.appendEntry(serializer.writeString(new NoOpEntry()));
    assertTrue(index == 1);
    assertTrue(log.containsEntry(1));
    log.removeAfter(0);
  }

  @Test
  public void testLoadEntry() {
    Log log = new FileLog();
    log.open("test");
    long index = log.appendEntry(serializer.writeString(new NoOpEntry()));
    Entry entry = serializer.readString(log.<String>getEntry(index), Entry.class);
    assertTrue(entry instanceof NoOpEntry);
    log.removeAfter(0);
  }

  @Test
  public void testFirstIndex() {
    Log log = new FileLog();
    log.open("test");
    long index;
    index = log.appendEntry(serializer.writeString(new NoOpEntry()));
    assertTrue(index == 1);
    index = log.appendEntry(serializer.writeString(new ConfigurationEntry()));
    assertTrue(index == 2);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 3);
    assertTrue(log.firstIndex() == 1);
    assertTrue(log.lastIndex() == 3);
    log.removeAfter(0);
  }

  @Test
  public void testFirstEntry() {
    Log log = new FileLog();
    log.open("test");
    long index;
    index = log.appendEntry(serializer.writeString(new NoOpEntry()));
    assertTrue(index == 1);
    index = log.appendEntry(serializer.writeString(new ConfigurationEntry()));
    assertTrue(index == 2);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 3);
    Entry entry = serializer.readString(log.<String>firstEntry(), Entry.class);
    assertTrue(entry instanceof NoOpEntry);
    log.removeAfter(0);
  }

  @Test
  public void testLastIndex() {
    Log log = new FileLog();
    log.open("test");
    long index;
    index = log.appendEntry(serializer.writeString(new NoOpEntry()));
    assertTrue(index == 1);
    index = log.appendEntry(serializer.writeString(new ConfigurationEntry()));
    assertTrue(index == 2);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 3);
    assertTrue(log.lastIndex() == 3);
    log.removeAfter(0);
  }

  @Test
  public void testLastEntry() {
    Log log = new FileLog();
    log.open("test");
    long index;
    index = log.appendEntry(serializer.writeString(new NoOpEntry()));
    assertTrue(index == 1);
    index = log.appendEntry(serializer.writeString(new ConfigurationEntry()));
    assertTrue(index == 2);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 3);
    Entry entry = serializer.readString(log.<String>lastEntry(), Entry.class);
    assertTrue(entry instanceof CommandEntry);
    log.removeAfter(0);
  }

  @Test
  public void testRemoveBefore() {
    Log log = new FileLog();
    log.open("test");
    long index;
    index = log.appendEntry(serializer.writeString(new NoOpEntry()));
    assertTrue(index == 1);
    index = log.appendEntry(serializer.writeString(new ConfigurationEntry()));
    assertTrue(index == 2);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 3);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 4);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 5);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 6);

    log.removeBefore(3);
    assertTrue(log.firstIndex() == 3);
    assertTrue(serializer.readString(log.<String>firstEntry(), Entry.class) instanceof CommandEntry);
    log.removeAfter(0);
  }

  @Test
  public void testRemoveAfter() {
    Log log = new FileLog();
    log.open("test");
    long index;
    index = log.appendEntry(serializer.writeString(new NoOpEntry()));
    assertTrue(index == 1);
    index = log.appendEntry(serializer.writeString(new ConfigurationEntry()));
    assertTrue(index == 2);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 3);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 4);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 5);
    index = log.appendEntry(serializer.writeString(new CommandEntry(1, "foo", new JsonObject())));
    assertTrue(index == 6);

    log.removeAfter(2);
    assertTrue(log.firstIndex() == 1);
    assertTrue(log.lastIndex() == 2);
    assertTrue(serializer.readString(log.<String>firstEntry(), Entry.class) instanceof NoOpEntry);
    assertTrue(serializer.readString(log.<String>lastEntry(), Entry.class) instanceof ConfigurationEntry);
    log.removeAfter(0);
  }

}
