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
package net.kuujo.copycat;

import java.util.ArrayList;
import java.util.HashSet;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.impl.CommandEntry;
import net.kuujo.copycat.log.impl.ConfigurationEntry;
import net.kuujo.copycat.log.impl.NoOpEntry;

import org.junit.Assert;
import org.junit.Test;


/**
 * Base log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class LogTest {

  /**
   * Creates a test log instance.
   */
  protected abstract Log createLog();

  @Test
  public void testAppendEntry() throws Exception {
    Log log = createLog();
    log.open();
    long index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    log.close();
  }

  @Test
  public void testContainsEntry() throws Exception {
    Log log = createLog();
    log.open();
    long index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    Assert.assertTrue(log.containsEntry(1));
    log.close();
  }

  @Test
  public void testGetEntry() throws Exception {
    Log log = createLog();
    log.open();
    long index = log.appendEntry(new NoOpEntry(1));
    Entry entry = log.getEntry(index);
    Assert.assertTrue(entry instanceof NoOpEntry);
    log.close();
  }

  @Test
  public void testFirstIndex() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new HashSet<>()));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 3);
    Assert.assertTrue(log.firstIndex() == 1);
    Assert.assertTrue(log.lastIndex() == 3);
    log.close();
  }

  @Test
  public void testFirstEntry() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new HashSet<>()));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 3);
    Entry entry = log.firstEntry();
    Assert.assertTrue(entry instanceof NoOpEntry);
    log.close();
  }

  @Test
  public void testLastIndex() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new HashSet<>()));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 3);
    Assert.assertTrue(log.lastIndex() == 3);
    log.close();
  }

  @Test
  public void testLastEntry() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new HashSet<>()));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 3);
    Entry entry = log.lastEntry();
    Assert.assertTrue(entry instanceof CommandEntry);
    log.close();
  }

  @Test
  public void testRemoveAfter() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new HashSet<>()));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 3);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 4);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 5);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 6);

    log.removeAfter(2);
    Assert.assertTrue(log.firstIndex() == 1);
    Assert.assertTrue(log.lastIndex() == 2);
    Assert.assertTrue(log.firstEntry() instanceof NoOpEntry);
    Assert.assertTrue(log.lastEntry() instanceof ConfigurationEntry);
    log.close();
  }

  @Test
  public void testManyOperations() throws Exception {
    Log log = createLog();
    log.open();
    long index;
    index = log.appendEntry(new NoOpEntry(1));
    Assert.assertTrue(index == 1);
    index = log.appendEntry(new ConfigurationEntry(1, new HashSet<>()));
    Assert.assertTrue(index == 2);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 3);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 4);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 5);
    index = log.appendEntry(new CommandEntry(1, "foo", new ArrayList<>()));
    Assert.assertTrue(index == 6);
    log.close();
  }

}
