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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * File log segment.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileLogSegment extends AbstractLogSegment {
  private static final int BUFFER_SIZE = 1024 * 2;
  private final FileLogManager log;
  private final File logFile;
  private final File indexFile;
  private final File metadataFile;
  private long timestamp;
  private FileChannel logFileChannel;
  private FileChannel indexFileChannel;
  private Long firstIndex;
  private Long lastIndex;
  private long indexOffset;
  private final ByteBuffer indexBuffer = ByteBuffer.allocate(8);
  private final ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
  private final List<ByteBuffer> pool = new ArrayList<>(1024);

  FileLogSegment(FileLogManager log, long id, long firstIndex) {
    super(id, firstIndex);
    this.log = log;
    this.logFile = new File(log.base.getParentFile(), String.format("%s-%d.log", log.base.getName(), id));
    this.indexFile = new File(log.base.getParentFile(), String.format("%s-%d.index", log.base.getName(), id));
    this.metadataFile = new File(log.base.getParentFile(), String.format("%s-%d.metadata", log.base.getName(), id));
  }

  @Override
  public LogManager log() {
    return log;
  }

  @Override
  public long timestamp() {
    assertIsOpen();
    return timestamp;
  }

  @Override
  public void open() throws IOException {
    assertIsNotOpen();
    if (!logFile.getParentFile().exists()) {
      logFile.getParentFile().mkdirs();
    }

    if (!metadataFile.exists()) {
      timestamp = System.currentTimeMillis();
      try (RandomAccessFile metaFile = new RandomAccessFile(metadataFile, "rw")) {
        metaFile.writeLong(super.firstIndex); // First index of the segment.
        metaFile.writeLong(timestamp); // Timestamp of the time at which the segment was created.
        metaFile.writeLong(0); // The segment index offset.
      }
    } else {
      try (RandomAccessFile metaFile = new RandomAccessFile(metadataFile, "r")) {
        if (metaFile.readLong() != super.firstIndex) {
          throw new LogException("Segment metadata out of sync");
        }
        timestamp = metaFile.readLong();
        indexOffset = metaFile.readLong();
      }
    }

    logFileChannel = FileChannel.open(this.logFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    logFileChannel.position(logFileChannel.size());
    indexFileChannel = FileChannel.open(this.indexFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    indexFileChannel.position(indexFileChannel.size());

    if (indexFileChannel.size() > 0) {
      firstIndex = super.firstIndex;
      lastIndex = firstIndex + indexFileChannel.size() / 8 - 1;
    }
  }

  @Override
  public boolean isEmpty() {
    assertIsOpen();
    return firstIndex == null;
  }

  @Override
  public boolean isOpen() {
    return logFileChannel != null && indexFileChannel != null;
  }

  @Override
  public long size() {
    assertIsOpen();
    try {
      return logFileChannel.size();
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  @Override
  public long entryCount() {
    assertIsOpen();
    return firstIndex != null ? lastIndex - firstIndex + 1 : 0;
  }

  /**
   * Returns the next log index.
   */
  private long nextIndex() {
    if (firstIndex == null) {
      firstIndex = super.firstIndex;
      lastIndex = firstIndex;
      return firstIndex;
    }
    return ++lastIndex;
  }

  @Override
  public long appendEntry(ByteBuffer entry) {
    assertIsOpen();
    long index = nextIndex();
    try {
      entry.rewind();
      long position = logFileChannel.position();
      logFileChannel.write(entry);
      storePosition(index, position);
    } catch (IOException e) {
      throw new LogException(e);
    }
    return index;
  }

  /**
   * Stores the position of an entry in the log.
   */
  private void storePosition(long index, long position) {
    try {
      ByteBuffer buffer = ByteBuffer.allocate(8).putLong(position);
      buffer.flip();
      indexFileChannel.write(buffer, (index - firstIndex) * 8);
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  /**
   * Finds the position of the given index in the segment.
   */
  private long findPosition(long index) {
    try {
      if (firstIndex == null || index <= firstIndex) {
        return 0;
      } else if (lastIndex == null || index > lastIndex) {
        return logFileChannel.size();
      }
      indexFileChannel.read(indexBuffer, (index - firstIndex) * 8);
      indexBuffer.flip();
      long position = indexBuffer.getLong() + indexOffset;
      indexBuffer.clear();
      return position;
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  @Override
  public Long firstIndex() {
    assertIsOpen();
    return firstIndex;
  }

  @Override
  public Long lastIndex() {
    assertIsOpen();
    return lastIndex;
  }

  @Override
  public boolean containsIndex(long index) {
    assertIsOpen();
    return firstIndex != null && lastIndex != null && firstIndex <= index && index <= lastIndex;
  }

  private ByteBuffer getBuffer(int index) {
    if (pool.size() > index) {
      ByteBuffer buffer = pool.get(index);
      buffer.clear();
      return buffer;
    } else {
      ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
      pool.add(buffer);
      return buffer;
    }
  }

  @Override
  public List<ByteBuffer> getEntries(long from, long to) {
    assertIsOpen();
    assertContainsIndex(from);
    assertContainsIndex(to);
    List<ByteBuffer> entries = new ArrayList<>((int)(to - from + 1));
    int index = 0;
    for (long i = from; i <= to; i++) {
      long startPosition = findPosition(i);
      long endPosition = findPosition(i + 1);
      try {
        ByteBuffer buffer = getBuffer(index+1);
        buffer.limit((int) (endPosition - startPosition));
        logFileChannel.read(buffer, startPosition);
        buffer.flip();
        entries.add(buffer);
      } catch (IOException e) {
        throw new LogException(e);
      }
    }
    return entries;
  }

  @Override
  public ByteBuffer getEntry(long index) {
    assertIsOpen();
    assertContainsIndex(index);
    try {
      long startPosition = findPosition(index);
      long endPosition = findPosition(index + 1);
      ByteBuffer buffer = getBuffer(0);
      buffer.limit((int) (endPosition - startPosition));
      logFileChannel.read(buffer, startPosition);
      buffer.flip();
      return buffer;
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  @Override
  public void removeAfter(long index) {
    assertIsOpen();
    if (containsIndex(index + 1)) {
      try {
        logFileChannel.truncate(findPosition(index + 1));
        indexFileChannel.truncate(((index + 1) - firstIndex) * 8);
        if (index >= firstIndex) {
          lastIndex = index;
        } else {
          lastIndex = null;
          firstIndex = null;
        }
      } catch (IOException e) {
        throw new LogException(e);
      }
    }
  }

  @Override
  public void compact(long index, ByteBuffer entry) {
    assertIsOpen();
    assertContainsIndex(index);
    try {
      // Create temporary log, index, and metadata files which will be copied to permanent names.
      File tempLogFile = new File(log.base.getParent(), String.format("%s-%d.log.tmp", log.base.getName(), id));
      File tempIndexFile = new File(log.base.getParent(), String.format("%s-%d.index.tmp", log.base.getName(), id));
      File tempMetadataFile = new File(log.base.getParent(), String.format("%s-%d.metadata.tmp", log.base.getName(), id));

      if (entry.position() == entry.limit()) {
        entry.flip();
      }

      // Calculate the offset of the entry index once the snapshot has been written to the log.
      long indexOffset;

      // Create temporary log, index, and metadata file channels for writing.
      try (FileChannel tempLogFileChannel = FileChannel.open(tempLogFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
          FileChannel tempIndexFileChannel = FileChannel.open(tempIndexFile.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
          FileChannel tempMetadataFileChannel = FileChannel.open(tempMetadataFile.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {

        // Transfer logs and indexes from the given index.
        tempLogFileChannel.write(entry);
        logFileChannel.transferTo(findPosition(index + 1), Integer.MAX_VALUE, tempLogFileChannel);

        indexFileChannel.transferTo((index - firstIndex) * 8, Integer.MAX_VALUE, tempIndexFileChannel);

        // Write the temporary metadata file.
        indexOffset = this.indexOffset + (tempLogFileChannel.size() - logFileChannel.size());
        ByteBuffer buffer = ByteBuffer.allocateDirect(24).putLong(index).putLong(timestamp).putLong(indexOffset);
        buffer.flip();
        tempMetadataFileChannel.write(buffer);

        // Flush temporary log, index, and metadata file contents to disk.
        tempLogFileChannel.force(true);
        tempIndexFileChannel.force(true);
        tempMetadataFileChannel.force(true);
      }

      // Create temporary files to which to copy current log, index, and metadata files.
      File historyLogFile = new File(log.base.getParent(), String.format("%s-%d.log.history", log.base.getName(), id));
      File historyIndexFile = new File(log.base.getParent(), String.format("%s-%d.index.history", log.base.getName(), id));
      File historyMetadataFile = new File(log.base.getParent(), String.format("%s-%d.metadata.history", log.base.getName(), id));

      // Copy current log, index, and metadata files to history files. History files will be recovered if a failure
      // occurs during the compaction.
      Files.copy(logFile.toPath(), historyLogFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      Files.copy(indexFile.toPath(), historyIndexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      Files.copy(metadataFile.toPath(), historyMetadataFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

      // Now rename temporary log, index, and metadata files to overwrite the current log, index, and metadata files.
      Files.move(tempLogFile.toPath(), logFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      Files.move(tempIndexFile.toPath(), indexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      Files.move(tempMetadataFile.toPath(), metadataFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

      // Finally, delete historical log, index, and metadata files.
      Files.delete(historyLogFile.toPath());
      Files.delete(historyIndexFile.toPath());
      Files.delete(historyMetadataFile.toPath());

      logFileChannel = FileChannel.open(logFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
      logFileChannel.position(logFileChannel.size());
      indexFileChannel = FileChannel.open(indexFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
      indexFileChannel.position(indexFileChannel.size());

      this.firstIndex = index;
      this.indexOffset = indexOffset;
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  @Override
  public void flush() {
    flush(false);
  }

  @Override
  public void flush(boolean force) {
    assertIsOpen();
    if (force || log.config.isFlushOnWrite()) {
      try {
        logFileChannel.force(true);
        indexFileChannel.force(true);
      } catch (IOException e) {
        throw new LogException(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    assertIsOpen();
    logFileChannel.close();
    logFileChannel = null;
    indexFileChannel.close();
    indexFileChannel = null;
  }

  @Override
  public boolean isClosed() {
    return logFileChannel == null;
  }

  @Override
  public void delete() {
    logFile.delete();
    indexFile.delete();
    metadataFile.delete();
  }

}
