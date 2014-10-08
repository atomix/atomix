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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import net.kuujo.copycat.internal.log.CopycatEntry;

/**
 * Memory-based log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InMemoryLog extends BaseLog implements Compactable {
  private TreeMap<Long, byte[]> log;
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
  public synchronized void open() {
    log = new TreeMap<>();
  }

  @Override
  public synchronized long size() {
    return log.size();
  }

  @Override
  public synchronized boolean isEmpty() {
    return log.isEmpty();
  }

  @Override
  public synchronized long appendEntry(Entry entry) {
    long index = log.isEmpty() ? 1 : log.lastKey() + 1;
    kryo.writeClassAndObject(output, entry);
    byte[] bytes = output.toBytes();
    log.put(index, bytes);
    output.clear();
    return index;
  }

  @Override
  public synchronized List<Long> appendEntries(Entry... entries) {
    List<Long> indices = new ArrayList<>(entries.length);
    for (Entry entry : entries) {
      indices.add(appendEntry(entry));
    }
    return indices;
  }

  @Override
  public synchronized List<Long> appendEntries(List<Entry> entries) {
    List<Long> indices = new ArrayList<>(entries.size());
    for (Entry entry : entries) {
      indices.add(appendEntry(entry));
    }
    return indices;
  }

  @Override
  public synchronized boolean containsEntry(long index) {
    return log.containsKey(index);
  }

  @Override
  public synchronized long firstIndex() {
    return !log.isEmpty() ? log.firstKey() : 0;
  }

  @Override
  public synchronized <T extends Entry> T firstEntry() {
    return !log.isEmpty() ? getEntry(log.firstKey()) : null;
  }

  @Override
  public synchronized long lastIndex() {
    return !log.isEmpty() ? log.lastKey() : 0;
  }

  @Override
  public synchronized <T extends Entry> T lastEntry() {
    return !log.isEmpty() ? getEntry(log.lastKey()) : null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T extends Entry> T getEntry(long index) {
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
  public synchronized <T extends Entry> List<T> getEntries(long from, long to) {
    List<T> entries = new ArrayList<>((int)(to - from + 1));
    for (long i = from; i <= to; i++) {
      T entry = getEntry(i);
      if (entry != null) {
        entries.add(entry);
      }
    }
    return entries;
  }

  @Override
  public synchronized void removeEntry(long index) {
    log.remove(index);
  }

  @Override
  public synchronized void removeAfter(long index) {
    log.tailMap(index, false).clear();
  }

  @Override
  public synchronized void compact(long index, Entry entry) throws IOException {
    kryo.writeClassAndObject(output, entry);
    byte[] bytes = output.toBytes();
    output.clear();
    log.headMap(index).clear();
    log.put(index, bytes);
  }

  @Override
  public void sync() {
  }

  @Override
  public void close() {
    log = null;
  }

  @Override
  public void delete() {
    log = null;
  }

  @Override
  public String toString() {
    return String.format("Follower[size=%d]", size());
  }

}
