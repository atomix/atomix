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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import net.kuujo.copycat.log.Compactable;
import net.kuujo.copycat.log.Entry;

/**
 * Memory-based log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemoryLog extends AbstractLog implements Compactable {
  private TreeMap<Long, ByteBuffer> log;

  public MemoryLog() {
    super(RaftEntry.class);
  }

  public MemoryLog(Class<? extends Entry> entryType) {
    super(entryType);
  }

  @Override
  public void open() throws IOException {
    log = new TreeMap<>();
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
  @SuppressWarnings("unchecked")
  public long appendEntry(Entry entry) {
    long index = log.isEmpty() ? 1 : log.lastKey() + 1;
    MemoryBuffer buffer = new MemoryBuffer();
    byte entryType = getEntryType(entry.getClass());
    buffer.appendByte(entryType);
    getWriter(entryType).writeEntry(entry, buffer);
    log.put(index, buffer.toByteBuffer());
    return index;
  }

  @Override
  public List<Long> appendEntries(Entry... entries) {
    List<Long> indices = new ArrayList<>();
    for (Entry entry : entries) {
      indices.add(appendEntry(entry));
    }
    return indices;
  }

  @Override
  public List<Long> appendEntries(List<Entry> entries) {
    List<Long> indices = new ArrayList<>();
    for (Entry entry : entries) {
      indices.add(appendEntry(entry));
    }
    return indices;
  }

  @Override
  public boolean containsEntry(long index) {
    return log.containsKey(index);
  }

  @Override
  public long firstIndex() {
    return !log.isEmpty() ? log.firstKey() : 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Entry> T firstEntry() {
    ByteBuffer byteBuffer = !log.isEmpty() ? log.firstEntry().getValue() : null;
    if (byteBuffer != null) {
      MemoryBuffer buffer = new MemoryBuffer(byteBuffer);
      byte entryType = buffer.getByte();
      return (T) getReader(entryType).readEntry(buffer);
    }
    return null;
  }

  @Override
  public long lastIndex() {
    return !log.isEmpty() ? log.lastKey() : 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Entry> T lastEntry() {
    ByteBuffer byteBuffer = !log.isEmpty() ? log.lastEntry().getValue() : null;
    if (byteBuffer != null) {
      MemoryBuffer buffer = new MemoryBuffer(byteBuffer);
      byte entryType = buffer.getByte();
      return (T) getReader(entryType).readEntry(buffer);
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Entry> T getEntry(long index) {
    ByteBuffer byteBuffer = log.get(index);
    if (byteBuffer != null) {
      MemoryBuffer buffer = new MemoryBuffer(byteBuffer);
      byte entryType = buffer.getByte();
      return (T) getReader(entryType).readEntry(buffer);
    }
    return null;
  }

  @Override
  public <T extends Entry> List<T> getEntries(long from, long to) {
    List<T> entries = new ArrayList<>();
    for (long i = from; i <= to; i++) {
      entries.add(getEntry(i));
    }
    return entries;
  }

  @Override
  public void removeEntry(long index) {
    log.remove(index);
  }

  @Override
  public void removeAfter(long index) {
    log.tailMap(index, false).clear();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void compact(long index, Entry entry) throws IOException {
    MemoryBuffer buffer = new MemoryBuffer();
    byte entryType = getEntryType(entry.getClass());
    buffer.appendByte(entryType);
    getWriter(entryType).writeEntry(entry, buffer);
    log.headMap(index).clear();
    log.put(index, buffer.toByteBuffer());
  }

  @Override
  public void close() {
    log = null;
  }

  @Override
  public void delete() {
    log = null;
  }

}
