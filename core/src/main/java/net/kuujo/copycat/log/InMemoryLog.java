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

import net.kuujo.copycat.internal.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Memory-based log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InMemoryLog extends BaseLog {
  private TreeMap<Long, byte[]> log;
  private volatile long size;

  public InMemoryLog() {
    this(new LogConfig());
  }

  public InMemoryLog(LogConfig config) {
    super(config);
  }

  @Override
  public LogConfig config() {
    return config;
  }

  @Override
  public synchronized void open() {
    assertIsNotOpen();
    log = new TreeMap<>();
  }

  @Override
  public boolean isOpen() {
    return log != null;
  }

  @Override
  public synchronized boolean isEmpty() {
    assertIsOpen();
    return log.isEmpty();
  }

  @Override
  public synchronized long size() {
    assertIsOpen();
    return size;
  }

  @Override
  public synchronized long appendEntry(Entry entry) {
    Assert.isNotNull(entry, "entry");
    assertIsOpen();

    long index = log.isEmpty() ? 1 : log.lastKey() + 1;
    byte[] bytes = serializer.writeObject(entry);
    log.put(index, bytes);
    size += bytes.length;
    return index;
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T extends Entry> T getEntry(long index) {
    assertIsOpen();

    byte[] bytes = log.get(index);
    if (bytes != null) {
      return serializer.readObject(bytes);
    }
    return null;
  }

  @Override
  public synchronized <T extends Entry> List<T> getEntries(long from, long to) {
    assertIsOpen();
    if (log.isEmpty()) {
      throw new IndexOutOfBoundsException("Log is empty");
    } else if (from < log.firstKey()) {
      throw new IndexOutOfBoundsException("From index out of bounds.");
    } else if (to > log.lastKey()) {
      throw new IndexOutOfBoundsException("To index out of bounds.");
    }

    List<T> entries = new ArrayList<>((int) (to - from + 1));
    for (long i = from; i <= to; i++) {
      T entry = getEntry(i);
      if (entry != null) {
        entries.add(entry);
      }
    }
    return entries;
  }

  @Override
  public synchronized long firstIndex() {
    assertIsOpen();
    return !log.isEmpty() ? log.firstKey() : 0;
  }

  @Override
  public synchronized <T extends Entry> T firstEntry() {
    assertIsOpen();
    return !log.isEmpty() ? getEntry(log.firstKey()) : null;
  }

  @Override
  public synchronized long lastIndex() {
    assertIsOpen();
    return !log.isEmpty() ? log.lastKey() : 0;
  }

  @Override
  public synchronized <T extends Entry> T lastEntry() {
    assertIsOpen();
    return !log.isEmpty() ? getEntry(log.lastKey()) : null;
  }

  @Override
  public synchronized void removeAfter(long index) {
    assertIsOpen();
    if (!log.isEmpty()) {
      for (long i = index + 1; i <= log.lastKey(); i++) {
        byte[] value = log.remove(i);
        if (value != null) {
          size -= value.length;
        }
      }
    }
  }

  @Override
  public void sync() {
    assertIsOpen();
  }

  @Override
  public void close() {
    assertIsOpen();
    log = null;
  }

  @Override
  public String toString() {
    return String.format("InMemoryLog[size=%d]", size());
  }

}
