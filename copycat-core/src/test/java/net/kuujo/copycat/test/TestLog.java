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
package net.kuujo.copycat.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiFunction;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.internal.log.CommandEntry;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.internal.log.NoOpEntry;
import net.kuujo.copycat.internal.log.SnapshotEntry;

/**
 * Test log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestLog extends InMemoryLog {
  private final TestLogEvents events;
  private final Set<EntryListener> entryListeners = new HashSet<>();
  private final Set<Runnable> compactListeners = new HashSet<>();

  private static class EntryListener {
    private final BiFunction<Long, Entry, Boolean> predicate;
    private final Runnable callback;
    private EntryListener(BiFunction<Long, Entry, Boolean> predicate, Runnable callback) {
      this.predicate = predicate;
      this.callback = callback;
    }
  }

  public TestLog() {
    events = new TestLogEvents(this);
    open();
  }

  /**
   * Adds an entry to the log.
   *
   * @param entry The entry to add.
   * @return The test log.
   */
  public TestLog withEntry(Entry entry) {
    appendEntry(entry);
    return this;
  }

  /**
   * Adds a no-op entry to the log.
   *
   * @param entry The entry to add.
   * @return The test log.
   */
  public TestLog withNoOpEntry(NoOpEntry entry) {
    appendEntry(entry);
    return this;
  }

  /**
   * Adds a configuration entry to the log.
   *
   * @param entry The entry to add.
   * @return The test log.
   */
  public TestLog withConfigurationEntry(ConfigurationEntry entry) {
    appendEntry(entry);
    return this;
  }

  /**
   * Adds many command entries to the log.
   *
   * @param numEntries The number of entries to add.
   * @param term The term in which to add the entries.
   * @return The test log.
   */
  public TestLog withCommandEntries(int numEntries, long term) {
    for (int i = 0; i < numEntries; i++) {
      appendEntry(new CommandEntry(term, "foo", Arrays.asList("bar", "baz")));
    }
    return this;
  }

  /**
   * Adds a snapshot entry to the log.
   *
   * @param entry The entry to add.
   * @return The test log.
   */
  public TestLog withSnapshotEntry(SnapshotEntry entry) {
    appendEntry(entry);
    return this;
  }

  /**
   * Returns test log event listeners.
   */
  public TestLogEvents await() {
    return events;
  }

  @Override
  public long appendEntry(Entry entry) {
    long index = super.appendEntry(entry);
    Iterator<EntryListener> iterator = entryListeners.iterator();
    while (iterator.hasNext()) {
      EntryListener listener = iterator.next();
      if (listener.predicate.apply(index, entry)) {
        iterator.remove();
        listener.callback.run();
      }
    }
    return index;
  }

  @Override
  public void compact(long index, Entry snapshot) throws IOException {
    super.compact(index, snapshot);
    for (Runnable callback : compactListeners) {
      callback.run();
    }
    compactListeners.clear();
  }

  void addEntryListener(BiFunction<Long, Entry, Boolean> function, Runnable callback) {
    entryListeners.add(new EntryListener(function, callback));
  }

  void addCompactListener(Runnable callback) {
    compactListeners.add(callback);
  }

}
