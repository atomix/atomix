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

import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import net.kuujo.copycat.internal.log.CopycatEntry;
import net.kuujo.copycat.internal.util.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Memory-based log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InMemoryLog extends BaseLog {
  private TreeMap<Long, byte[]> log;
  private volatile long size;
  private final ByteBuffer buffer = ByteBuffer.allocate(4096);
  private final ByteBufferOutput output = new ByteBufferOutput(buffer);
  private final ByteBufferInput input = new ByteBufferInput(buffer);

  public InMemoryLog() {
    super(CopycatEntry.class);
  }

  public InMemoryLog(Class<? extends Entry> entryType) {
    super(entryType);
  }

  @Override
  public synchronized long appendEntry(Entry entry) {
    Assert.isNotNull(entry, "entry");
    assertIsOpen();

    long index = log.isEmpty() ? 1 : log.lastKey() + 1;
    kryo.writeClassAndObject(output, entry);
    byte[] bytes = output.toBytes();
    log.put(index, bytes);
    size += bytes.length;
    output.clear();
    return index;
  }

  @Override
  public void close() {
    assertIsOpen();
    log = null;
  }

  @Override
  public synchronized void compact(long index, Entry entry) throws IOException {
    Assert.isNotNull(entry, "entry");
    assertIsOpen();

    kryo.writeClassAndObject(output, entry);
    byte[] bytes = output.toBytes();
    output.clear();
    // TODO - calculate newSize by doing the lesser of subtracting out removed entries or adding
    // remaining entries.
    log.headMap(index).clear();
    log.put(index, bytes);
    long newSize = 0;
    for (Map.Entry<Long, byte[]> e : log.entrySet()) {
      newSize += e.getValue().length;
    }
    size = newSize;
  }

  @Override
  public synchronized boolean containsEntry(long index) {
    assertIsOpen();
    return log.containsKey(index);
  }

  @Override
  public void delete() {
    log = null;
  }

  @Override
  public synchronized <T extends Entry> T firstEntry() {
    assertIsOpen();
    return !log.isEmpty() ? getEntry(log.firstKey()) : null;
  }

  @Override
  public synchronized long firstIndex() {
    assertIsOpen();
    return !log.isEmpty() ? log.firstKey() : 0;
  }

  @Override
  public synchronized <T extends Entry> List<T> getEntries(long from, long to) {
    assertIsOpen();
    if (log.isEmpty()) {
      throw new LogIndexOutOfBoundsException("Log is empty");
    } else if (from < log.firstKey()) {
      throw new LogIndexOutOfBoundsException("From index out of bounds.");
    } else if (to > log.lastKey()) {
      throw new LogIndexOutOfBoundsException("To index out of bounds.");
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
  @SuppressWarnings("unchecked")
  public synchronized <T extends Entry> T getEntry(long index) {
    assertIsOpen();

    byte[] bytes = log.get(index);
    if (bytes != null) {
      buffer.put(bytes);
      buffer.rewind();
      input.setBuffer(buffer);
      T entry = (T) kryo.readClassAndObject(input);
      buffer.clear();
      return entry;
    }
    return null;
  }

  @Override
  public synchronized boolean isEmpty() {
    assertIsOpen();
    return log.isEmpty();
  }

  @Override
  public boolean isOpen() {
    return log != null;
  }

  @Override
  public synchronized <T extends Entry> T lastEntry() {
    assertIsOpen();
    return !log.isEmpty() ? getEntry(log.lastKey()) : null;
  }

  @Override
  public synchronized long lastIndex() {
    assertIsOpen();
    return !log.isEmpty() ? log.lastKey() : 0;
  }

  @Override
  public synchronized void open() {
    assertIsNotOpen();
    log = new TreeMap<>();
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
  public synchronized long size() {
    assertIsOpen();
    return size;
  }

  @Override
  public void sync() {
    assertIsOpen();
  }

  @Override
  public String toString() {
    return String.format("Follower[size=%d]", size());
  }

}
