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
package net.kuujo.copycat.log.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.EntryEvent;
import net.kuujo.copycat.log.EntryListener;
import net.kuujo.copycat.log.Log;

/**
 * In-memory log implementation.<p>
 *
 * This log implementation uses an in-memory {@link TreeMap} to store
 * log entries in log order. While this log is intended for testing
 * purposes, it can be used in production in cases where snapshots
 * suffice to maintain a small memory footprint.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemoryLog implements Log {
  private TreeMap<Long, Entry> log = new TreeMap<>();
  private final Set<EntryListener> listeners = new HashSet<>();

  @Override
  public void addListener(EntryListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(EntryListener listener) {
    listeners.remove(listener);
  }

  @Override
  public void open() {
  }

  @Override
  public long size() {
    return log.size();
  }

  @Override
  public boolean isEmpty() {
    return log.isEmpty();
  }

  private void triggerAddEvent(long index, Entry entry) {
    if (!listeners.isEmpty()) {
      EntryEvent event = new EntryEvent(index, entry);
      for (EntryListener listener : listeners) {
        listener.entryAdded(event);
      }
    }
  }

  @Override
  public synchronized long appendEntry(Entry entry) {
    if (entry == null) throw new NullPointerException();
    long index = (!log.isEmpty() ? log.lastKey() : 0) + 1;
    log.put(index, entry);
    triggerAddEvent(index, entry);
    return index;
  }

  @Override
  public synchronized List<Long> appendEntries(Entry... entries) {
    List<Long> indices = new ArrayList<>();
    for (Entry entry : entries) {
      indices.add(appendEntry(entry));
    }
    return indices;
  }

  @Override
  public synchronized List<Long> appendEntries(List<? extends Entry> entries) {
    List<Long> indices = new ArrayList<>();
    for (Entry entry : entries) {
      indices.add(appendEntry(entry));
    }
    return indices;
  }

  @Override
  public long setEntry(long index, Entry entry) {
    log.put(index, entry);
    return index;
  }

  @Override
  public long prependEntry(Entry entry) {
    long index = (!log.isEmpty() ? log.firstKey() : 0) - 1;
    if (index < 1) {
      throw new IndexOutOfBoundsException("Cannot prepend entry at index " + index);
    }
    log.put(index, entry);
    return index;
  }

  @Override
  public synchronized List<Long> prependEntries(Entry... entries) {
    return prependEntries(Arrays.asList(entries));
  }

  @Override
  public synchronized List<Long> prependEntries(List<? extends Entry> entries) {
    List<Long> indices = new ArrayList<>();
    for (int i = entries.size() - 1; i >= 0; i--) {
      indices.add(prependEntry(entries.get(i)));
    }
    return indices;
  }

  @Override
  public boolean containsEntry(long index) {
    return log.containsKey(index);
  }

  @Override
  public Entry getEntry(long index) {
    return log.get(index);
  }

  @Override
  public long firstIndex() {
    return !log.isEmpty() ? log.firstKey() : 0;
  }

  @Override
  public Entry firstEntry() {
    return !log.isEmpty() ? log.firstEntry().getValue() : null;
  }

  @Override
  public long lastIndex() {
    return !log.isEmpty() ? log.lastKey() : 0;
  }

  @Override
  public Entry lastEntry() {
    return !log.isEmpty() ? log.lastEntry().getValue() : null;
  }

  @Override
  public synchronized List<Entry> getEntries(long start, long end) {
    List<Entry> entries = new ArrayList<>();
    for (Map.Entry<Long, Entry> entry : log.subMap(start, end+1).entrySet()) {
      entries.add(entry.getValue());
    }
    return entries;
  }

  @Override
  public synchronized void removeBefore(long index) {
    try {
      long firstKey;
      while ((firstKey = log.firstKey()) < index) {
        log.remove(firstKey);
      }
    } catch (NoSuchElementException e) {
    }
  }

  @Override
  public synchronized void removeAfter(long index) {
    try {
      long lastKey;
      while ((lastKey = log.lastKey()) > index) {
        log.remove(lastKey);
      }
    } catch (NoSuchElementException e) {
    }
  }

  @Override
  public void close() {
  }

  @Override
  public synchronized void delete() {
    log = new TreeMap<>();
  }

}
