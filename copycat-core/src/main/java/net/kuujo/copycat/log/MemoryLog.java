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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * A default log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemoryLog implements Log {
  private TreeMap<Long, Entry> log = new TreeMap<>();

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

  @Override
  public long appendEntry(Entry entry) {
    long index = (!log.isEmpty() ? log.lastKey() : 0) + 1;
    log.put(index, entry);
    return index;
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
  public Log setEntry(long index, Entry entry) {
    log.put(index, entry);
    return this;
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
  public List<Entry> getEntries(long start, long end) {
    List<Entry> entries = new ArrayList<>();
    for (Map.Entry<Long, Entry> entry : log.subMap(start, end+1).entrySet()) {
      entries.add(entry.getValue());
    }
    return entries;
  }

  @Override
  public void removeBefore(long index) {
    try {
      long firstKey;
      while ((firstKey = log.firstKey()) < index) {
        log.remove(firstKey);
      }
    } catch (NoSuchElementException e) {
    }
  }

  @Override
  public void removeAfter(long index) {
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
  public void delete() {
    log = new TreeMap<>();
  }

}
