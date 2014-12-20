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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;

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

  private final ChronicleLog parent;
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

  ChronicleLogSegment(ChronicleLog parent, long id, long firstIndex) {
    super(id, firstIndex);
    this.parent = parent;
    this.basePath = new File(parent.base().getParent(), String.format("%s-%d", parent.base().getName(), id));
    this.dataFile = new File(parent.base().getParent(), String.format("%s-%d.data", parent.base().getName(), id));
    this.indexFile = new File(parent.base().getParent(), String.format("%s-%d.index", parent.base().getName(), id));
  }

  @Override
  public Log log() {
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
  public long entries() {
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
  public List<Long> appendEntries(List<ByteBuffer> entries) {
    assertIsOpen();
    List<Long> indices = new ArrayList<>(entries.size());
    for (ByteBuffer entry : entries) {
      indices.add(appendEntry(entry));
    }
    return indices;
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
          return entry;
        }
      } while (tailer.nextIndex());
    }
    return null;
  }

  @Override
  public List<ByteBuffer> getEntries(long from, long to) {
    assertIsOpen();
    assertContainsIndex(from);
    assertContainsIndex(to);
    List<ByteBuffer> entries = new ArrayList<>((int) (to - from + 1));
    long currentIndex = from;
    if (tailer.index(from - firstIndex)) {
      do {
        ByteBuffer entry = extractEntry(tailer, currentIndex);
        if (entry != null) {
          entries.add(entry);
          currentIndex++;
        }
        if (currentIndex > to) {
          return entries;
        }
      } while (tailer.nextIndex());
    }
    return entries;
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
  public void compact(long index, ByteBuffer entry) {
    assertIsOpen();
    assertContainsIndex(index);

    // Create a new log file using the most recent timestamp.
    File tempBaseFile = new File(basePath.getParent(), String.format("%s-%d.tmp", basePath.getName(), id));
    File tempDataFile = new File(basePath.getParent(), String.format("%s-%d.tmp.data", basePath.getName(), id));
    File tempIndexFile = new File(basePath.getParent(), String.format("%s-%d.tmp.index", basePath.getName(), id));
    int newSize = 0;
    int newEntries = 0;

    // Create a new chronicle for the new log file.
    try (Chronicle tempChronicle = new IndexedChronicle(tempBaseFile.getAbsolutePath(), parent.chronicleConfig);
      ExcerptAppender tempAppender = tempChronicle.createAppender()) {

      long copycatIndex = index;
      long chronicleIndex = index - firstIndex;

      // If an entry is to replace the existing entry at the given index, write the new entry
      // first.
      if (entry != null) {
        if (entry.remaining() == 0)
          entry.flip();
        tempAppender.startExcerpt();
        tempAppender.writeLong(copycatIndex);
        tempAppender.writeByte(ACTIVE);
        tempAppender.writeInt(entry.limit());
        tempAppender.write(entry);
        tempAppender.finish();
        newSize += entry.limit() + ENTRY_INFO_LEN;
        newEntries++;
        copycatIndex++;
        chronicleIndex++;
      }

      // Iterate through entries greater than the given index and copy them to the new chronicle.
      if (tailer.index(chronicleIndex)) {
        do {
          ByteBuffer currentEntry = extractEntry(tailer, copycatIndex);
          if (currentEntry != null) {
            if (currentEntry.remaining() == 0)
              currentEntry.flip();
            tempAppender.startExcerpt();
            tempAppender.writeLong(copycatIndex);
            tempAppender.writeByte(ACTIVE);
            tempAppender.writeInt(currentEntry.limit());
            tempAppender.write(currentEntry);
            tempAppender.finish();
            newSize += currentEntry.limit() + ENTRY_INFO_LEN;
            newEntries++;
            copycatIndex++;
            chronicleIndex++;
          }
        } while (tailer.nextIndex());
      }

      // Close the existing chronicle.
      this.excerpt.close();
      this.appender.close();
      this.tailer.close();
      this.chronicle.close();

      // First, create a copy of the existing log files. This can be used to restore the logs
      // during recovery if the compaction fails.
      File historyDataFile = new File(basePath.getParent(), String.format("%s-%d.history.data", basePath.getName(), id));
      File historyIndexFile = new File(basePath.getParent(), String.format("%s-%d.history.index", basePath.getName(), id));
      Files.copy(dataFile.toPath(), historyDataFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      Files.copy(indexFile.toPath(), historyIndexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

      // Now rename temporary log files.
      Files.move(tempDataFile.toPath(), dataFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      Files.move(tempIndexFile.toPath(), indexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

      // Delete the history files if we've made it this far.
      historyDataFile.delete();
      historyIndexFile.delete();

      // Reset chronicle log types.
      this.chronicle = new IndexedChronicle(basePath.getAbsolutePath(), parent.chronicleConfig);
      this.excerpt = chronicle.createExcerpt();
      this.appender = chronicle.createAppender();
      this.tailer = chronicle.createTailer();
      this.firstIndex = index;
      this.size = newSize;
      this.entries = newEntries;
    } catch (IOException e) {
      throw new LogException(e, "Failed to compact log segment at index %s", index);
    }
  }

  @Override
  public void flush() {
    flush(false);
  }

  @Override
  public void flush(boolean force) {
    assertIsOpen();
    if (force || parent.config.isFlushOnWrite()) {
      excerpt.flush();
      appender.flush();
      tailer.flush();
    }
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
