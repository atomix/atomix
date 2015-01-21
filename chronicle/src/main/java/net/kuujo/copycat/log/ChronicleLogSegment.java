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

import net.openhft.chronicle.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Chronicle based log segment.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ChronicleLogSegment extends AbstractLogSegment {
  private static final byte DELETED = 0;
  private static final byte ACTIVE = 1;
  /* Size of index + status + length data */
  private static final int ENTRY_INFO_LEN = 13;

  private final ChronicleLogManager parent;
  /* The base path to chronicle files */
  private final File basePath;
  private final File dataFile;
  private final File indexFile;
  private Chronicle chronicle;
  private Excerpt excerpt;
  private ExcerptAppender appender;
  private ExcerptTailer tailer;
  private Long lastIndex;
  private long size;
  private long entries;

  ChronicleLogSegment(ChronicleLogManager parent, long id, long firstIndex) {
    super(id, firstIndex);
    this.parent = parent;
    this.basePath = new File(parent.base.getParent(), String.format("%s-%d", parent.base.getName(), id));
    this.dataFile = new File(parent.base.getParent(), String.format("%s-%d.data", parent.base.getName(), id));
    this.indexFile = new File(parent.base.getParent(), String.format("%s-%d.index", parent.base.getName(), id));
  }

  @Override
  public LogManager log() {
    return parent;
  }

  @Override
  public long timestamp() {
    try {
      BasicFileAttributes attributes = Files.readAttributes(dataFile.toPath(), BasicFileAttributes.class);
      return attributes.creationTime().toMillis();
    } catch (IOException e) {
      throw new LogException(e, "Failed to read Chronicle segment data file: %s", dataFile);
    }
  }

  @Override
  public void open() throws IOException {
    assertIsNotOpen();

    chronicle = new IndexedChronicle(basePath.getAbsolutePath(), parent.chronicleConfig);
    excerpt = chronicle.createExcerpt();
    appender = chronicle.createAppender();
    tailer = chronicle.createTailer();

    if (chronicle.size() > 0) {
      try (ExcerptTailer t = tailer.toStart()) {
        do {
          long index = t.readLong();
          if (t.readByte() == ACTIVE) {
            lastIndex = index;
          }
        } while (t.nextIndex());
      }
    }
  }

  @Override
  public boolean isOpen() {
    return chronicle != null;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public long size() {
    assertIsOpen();
    return size;
  }

  @Override
  public long entryCount() {
    assertIsOpen();
    return entries;
  }

  @Override
  public long appendEntry(ByteBuffer entry) {
    assertIsOpen();
    long index = lastIndex == null ? firstIndex : lastIndex + 1;
    if (entry.remaining() == 0)
      entry.flip();
    appender.startExcerpt();
    appender.writeLong(index);
    appender.writeByte(ACTIVE);
    appender.writeInt(entry.limit());
    appender.write(entry);
    appender.finish();
    lastIndex = index;
    size += entry.capacity() + ENTRY_INFO_LEN;
    entries++;
    return index;
  }

  @Override
  public Long firstIndex() {
    assertIsOpen();
    return chronicle.size() == 0 ? null : firstIndex;
  }

  @Override
  public Long lastIndex() {
    assertIsOpen();
    return lastIndex;
  }

  @Override
  public boolean containsIndex(long index) {
    assertIsOpen();
    return !isEmpty() && firstIndex <= index && index <= lastIndex;
  }

  @Override
  public ByteBuffer getEntry(long index) {
    assertIsOpen();
    assertContainsIndex(index);
    if (tailer.index(index - firstIndex)) {
      do {
        ByteBuffer entry = extractEntry(tailer, index);
        if (entry != null) {
          entry.flip();
          return entry;
        }
      } while (tailer.nextIndex());
    }
    return null;
  }

  /**
   * Extracts an entry from the excerpt.
   */
  private ByteBuffer extractEntry(ExcerptTailer excerpt, long matchIndex) {
    long index = excerpt.readLong();
    byte status = excerpt.readByte();
    if (status == DELETED)
      return null;
    if (index == matchIndex && status == ACTIVE) {
      int length = excerpt.readInt();
      ByteBuffer buffer = ByteBuffer.allocate(length);
      excerpt.read(buffer);
      return buffer;
    } else if (index > matchIndex) {
      throw new IllegalStateException("Log missing entries");
    }
    return null;
  }

  @Override
  public void removeAfter(long index) {
    assertIsOpen();
    if (index < firstIndex) {
      chronicle.clear();
      size = 0;
      entries = 0;
    } else if (excerpt.index(index - firstIndex)) {
      while (excerpt.nextIndex()) {
        if (excerpt.readLong() > index) {
          excerpt.writeByte(DELETED);
          int entrySize = excerpt.readInt();
          size -= (entrySize + ENTRY_INFO_LEN);
          entries--;
        }
      }
    }
    lastIndex = index;
  }

  @Override
  public void flush() {
    assertIsOpen();
    excerpt.flush();
    appender.flush();
  }

  @Override
  public void close() throws IOException {
    assertIsOpen();
    try {
      chronicle.close();
    } finally {
      chronicle = null;
      excerpt = null;
      lastIndex = null;
    }
  }

  @Override
  public boolean isClosed() {
    return chronicle == null;
  }

  @Override
  public void delete() {
    if (isOpen()) {
      try {
        close();
      } catch (IOException ignore) {
      }
    }

    dataFile.delete();
    indexFile.delete();
  }
}
