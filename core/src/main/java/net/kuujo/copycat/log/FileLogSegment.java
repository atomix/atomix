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
import java.nio.file.StandardOpenOption;

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
  private final ByteBuffer indexBuffer = ByteBuffer.allocate(8);

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
      }
    } else {
      try (RandomAccessFile metaFile = new RandomAccessFile(metadataFile, "r")) {
        if (metaFile.readLong() != super.firstIndex) {
          throw new LogException("Segment metadata out of sync");
        }
        timestamp = metaFile.readLong();
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
      long position = indexBuffer.getLong();
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

  @Override
  public ByteBuffer getEntry(long index) {
    assertIsOpen();
    assertContainsIndex(index);
    try {
      long startPosition = findPosition(index);
      long endPosition = findPosition(index + 1);
      ByteBuffer buffer = ByteBuffer.allocate((int) (endPosition - startPosition));
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
  public void flush() {
    try {
      logFileChannel.force(false);
      indexFileChannel.force(false);
    } catch (IOException e) {
      throw new LogException(e);
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
