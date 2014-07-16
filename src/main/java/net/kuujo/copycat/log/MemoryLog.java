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

import org.vertx.java.core.Handler;

/**
 * A default log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemoryLog implements Log {
  private static final long DEFAULT_MAX_SIZE = 5000; // Default 5000 log entries.
  private TreeMap<Long, Entry> log = new TreeMap<>();
  private long maxSize = DEFAULT_MAX_SIZE;
  private Handler<Void> fullHandler;
  private Handler<Void> drainHandler;
  private boolean full;

  @Override
  public void open() {
  }

  @Override
  public Log setMaxSize(long maxSize) {
    this.maxSize = maxSize;
    return this;
  }

  @Override
  public long getMaxSize() {
    return maxSize;
  }

  @Override
  public Log fullHandler(Handler<Void> handler) {
    fullHandler = handler;
    return this;
  }

  @Override
  public Log drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  @Override
  public long appendEntry(Entry entry) {
    long index = (!log.isEmpty() ? log.lastKey() : 0) + 1;
    log.put(index, entry);
    checkFull();
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
      checkFull();
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
      checkFull();
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

  /**
   * Checks whether the log is full.
   */
  private void checkFull() {
    if (!full) {
      if (log.size() >= maxSize) {
        full = true;
        if (fullHandler != null) {
          fullHandler.handle((Void) null);
        }
      }
    } else {
      if (log.size() < maxSize) {
        full = false;
        if (drainHandler != null) {
          drainHandler.handle((Void) null);
        }
      }
    }
  }

}
