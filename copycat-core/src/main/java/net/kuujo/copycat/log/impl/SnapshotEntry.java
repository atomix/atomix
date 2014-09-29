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

import java.util.HashSet;
import java.util.Set;

import net.kuujo.copycat.log.Buffer;
import net.kuujo.copycat.log.EntryReader;
import net.kuujo.copycat.log.EntryType;
import net.kuujo.copycat.log.EntryWriter;

/**
 * Snapshot log entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@EntryType(id=3, reader=SnapshotEntry.Reader.class, writer=SnapshotEntry.Writer.class)
public class SnapshotEntry extends RaftEntry {
  private Set<String> config;
  private byte[] data;

  private SnapshotEntry() {
    super();
  }

  public SnapshotEntry(long term, Set<String> config, byte[] data) {
    super(term);
    this.config = config;
    this.data = data;
  }

  /**
   * Returns the snapshot configuration.
   *
   * @return The snapshot configuration.
   */
  public Set<String> config() {
    return config;
  }

  /**
   * Returns the snapshot data.
   *
   * @return The snapshot data.
   */
  public byte[] data() {
    return data;
  }

  @Override
  public String toString() {
    return String.format("SnapshotEntry[term=%d, config=%s, data=...]", term, config);
  }

  public static class Reader implements EntryReader<SnapshotEntry> {
    @Override
    public SnapshotEntry readEntry(Buffer buffer) {
      SnapshotEntry entry = new SnapshotEntry();
      entry.term = buffer.getLong();
      entry.config = buffer.getCollection(new HashSet<String>(), String.class);
      int length = buffer.getInt();
      entry.data = buffer.getBytes(length);
      return entry;
    }
  }

  public static class Writer implements EntryWriter<SnapshotEntry> {
    @Override
    public void writeEntry(SnapshotEntry entry, Buffer buffer) {
      buffer.appendLong(entry.term);
      buffer.appendCollection(entry.config);
      buffer.appendInt(entry.data.length);
      buffer.appendBytes(entry.data);
    }
  }

}
