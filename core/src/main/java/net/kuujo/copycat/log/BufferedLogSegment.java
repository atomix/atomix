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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory log segment.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferedLogSegment extends AbstractLogger implements LogSegment {
  private final BufferedLog parent;
  private final long segment;
  private final Lock lock = new ReentrantLock();
  private final AtomicBoolean locked = new AtomicBoolean();
  private long timestamp;
  private TreeMap<Long, ByteBuffer> log;
  private int size;

  BufferedLogSegment(BufferedLog parent, long segment) {
    this.parent = parent;
    this.segment = segment;
  }

  @Override
  public Log log() {
    return parent;
  }

  @Override
  public File file() {
    return null;
  }

  @Override
  public File index() {
    return null;
  }

  @Override
  public long segment() {
    return segment;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public void lock() {
    lock.lock();
    locked.set(true);
  }

  @Override
  public boolean isLocked() {
    return locked.get();
  }

  @Override
  public void unlock() {
    lock.unlock();
    locked.set(false);
  }

  @Override
  public void open() {
    assertIsNotOpen();
    log = new TreeMap<>();
    timestamp = System.currentTimeMillis();
    size = 0;
  }

  @Override
  public boolean isOpen() {
    return log != null;
  }

  @Override
  public int size() {
    assertIsOpen();
    return size;
  }

  @Override
  public long appendEntry(ByteBuffer entry) {
    Assert.isNotNull(entry, "entry");
    assertIsOpen();
    long index = log.isEmpty() ? 1 : log.lastKey() + 1;
    log.put(index, entry);
    size += entry.limit();
    return index;
  }

  @Override
  public List<Long> appendEntries(List<ByteBuffer> entries) {
    assertIsOpen();
    return null;
  }

  @Override
  public long firstIndex() {
    assertIsOpen();
    return !log.isEmpty() ? log.firstKey() : 0;
  }

  @Override
  public long lastIndex() {
    assertIsOpen();
    return !log.isEmpty() ? log.lastKey() : 0;
  }

  @Override
  public boolean containsIndex(long index) {
    assertIsOpen();
    return log.containsKey(index);
  }

  @Override
  public ByteBuffer getEntry(long index) {
    assertIsOpen();
    assertContainsIndex(index);
    return log.get(index);
  }

  @Override
  public List<ByteBuffer> getEntries(long from, long to) {
    assertIsOpen();
    assertContainsIndex(from);
    assertContainsIndex(to);

    List<ByteBuffer> entries = new ArrayList<>((int) (to - from + 1));
    for (long i = from; i <= to; i++) {
      ByteBuffer entry = getEntry(i);
      if (entry != null) {
        entries.add(entry);
      }
    }
    return entries;
  }

  @Override
  public void removeAfter(long index) {
    assertIsOpen();
    assertContainsIndex(index);
    for (long i = index + 1; i <= log.lastKey(); i++) {
      ByteBuffer value = log.remove(i);
      if (value != null) {
        size -= value.limit();
      }
    }
  }

  @Override
  public void compact(long index) {
    assertIsOpen();
    if (!log.isEmpty()) {
      if (index < log.firstKey()) {
        throw new IllegalArgumentException("Log does not contain index " + index);
      } else if (index > log.lastKey()) {
        log.clear();
      } else {
        for (long i = log.firstKey(); i < index; i++) {
          ByteBuffer value = log.remove(i);
          if (value != null) {
            size -= value.limit();
          }
        }
      }
    }
  }

  @Override
  public void compact(long index, ByteBuffer entry) {
    assertIsOpen();
    assertContainsIndex(index);
    log.put(index, entry);
    if (log.firstKey() != index) {
      for (long i = log.firstKey(); i < index; i++) {
        ByteBuffer value = log.remove(i);
        if (value != null) {
          size -= value.limit();
        }
      }
    }
  }

  @Override
  public void flush() {
    assertIsOpen();
  }

  @Override
  public void flush(boolean force) {
    assertIsOpen();
  }

  @Override
  public void close() {
    assertIsOpen();
    log = null;
    timestamp = 0;
    size = 0;
  }

  @Override
  public boolean isClosed() {
    return log == null;
  }

  @Override
  public void delete() {
    if (log != null) {
      log.clear();
    }
    parent.deleteSegment(segment);
  }

}
