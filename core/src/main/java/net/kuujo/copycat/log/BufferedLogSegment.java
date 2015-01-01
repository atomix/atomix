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

import java.nio.ByteBuffer;
import java.util.TreeMap;

import net.kuujo.copycat.internal.util.Assert;

/**
 * In-memory log segment.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferedLogSegment extends AbstractLogSegment {
  private final BufferedLogManager parent;
  private long timestamp;
  private TreeMap<Long, ByteBuffer> log;
  private int size;

  BufferedLogSegment(BufferedLogManager parent, long id, long firstIndex) {
    super(id, firstIndex);
    this.parent = parent;
  }

  @Override
  public LogManager log() {
    return parent;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public void open() {
    assertIsNotOpen();
    if (log == null) {
      log = new TreeMap<>();
      size = 0;
      timestamp = System.currentTimeMillis();
    }
  }

  @Override
  public boolean isOpen() {
    return log != null;
  }
  
  @Override
  public boolean isEmpty() {
    return log != null && !log.isEmpty();
  }

  @Override
  public long size() {
    assertIsOpen();
    return size;
  }

  @Override
  public long entryCount() {
    assertIsOpen();
    return log.size();
  }

  @Override
  public long appendEntry(ByteBuffer entry) {
    Assert.isNotNull(entry, "entry");
    assertIsOpen();
    long index = log.isEmpty() ? firstIndex : log.lastKey() + 1;
    log.put(index, entry);
    size += entry.limit();
    return index;
  }

  @Override
  public Long firstIndex() {
    assertIsOpen();
    return !log.isEmpty() ? log.firstKey() : null;
  }

  @Override
  public Long lastIndex() {
    assertIsOpen();
    return !log.isEmpty() ? log.lastKey() : null;
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
  public void removeAfter(long index) {
    assertIsOpen();
    if (index < firstIndex) {
      log.clear();
      size = 0;
    } else {
      assertContainsIndex(index);
      for (long i = index + 1; i <= log.lastKey(); i++) {
        ByteBuffer value = log.remove(i);
        if (value != null) {
          size -= value.limit();
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
  }

  @Override
  public boolean isClosed() {
    return log == null;
  }

  @Override
  public void delete() {
    if (log != null) {
      log.clear();
      log = null;
    }
  }
}
