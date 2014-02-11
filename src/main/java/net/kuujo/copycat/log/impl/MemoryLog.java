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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.kuujo.copycat.log.Log;

import org.vertx.java.core.Handler;

/**
 * A default log implementation.
 * 
 * @author Jordan Halterman
 */
public class MemoryLog implements Log {
  private static final long DEFAULT_MAX_SIZE = 5000; // Default 5000 log entries.
  private final TreeMap<Long, Object> log = new TreeMap<>();
  private long maxSize = DEFAULT_MAX_SIZE;
  private Handler<Void> fullHandler;
  private Handler<Void> drainHandler;
  private boolean full;

  @Override
  public void open(String filename) {
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
  public <T> long appendEntry(T entry) {
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
  @SuppressWarnings("unchecked")
  public <T> T getEntry(long index) {
    return (T) log.get(index);
  }

  @Override
  public long firstIndex() {
    return !log.isEmpty() ? log.firstKey() : 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T firstEntry() {
    return (T) (!log.isEmpty() ? log.firstEntry().getValue() : null);
  }

  @Override
  public long lastIndex() {
    return !log.isEmpty() ? log.lastKey() : 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T lastEntry() {
    return (T) (!log.isEmpty() ? log.lastEntry().getValue() : null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> getEntries(long start, long end) {
    List<T> entries = new ArrayList<>();
    for (Map.Entry<Long, Object> entry : log.subMap(start, end+1).entrySet()) {
      entries.add((T) entry.getValue());
    }
    return entries;
  }

  @Override
  public void removeBefore(long index) {
    long firstKey;
    while ((firstKey = log.firstKey()) < index) {
      log.remove(firstKey);
    }
    checkFull();
  }

  @Override
  public void removeAfter(long index) {
    long lastKey;
    while ((lastKey = log.lastKey()) > index) {
      log.remove(lastKey);
    }
    checkFull();
  }

  @Override
  public void close() {
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
    }
    else {
      if (log.size() < maxSize) {
        full = false;
        if (drainHandler != null) {
          drainHandler.handle((Void) null);
        }
      }
    }
  }

}
