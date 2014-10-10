/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;

import net.kuujo.copycat.internal.log.CopycatEntry;
import net.kuujo.copycat.internal.util.Assert;

import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;

/**
 * Random access file log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileLog extends BaseFileLog implements Compactable {
  private static final String SEPARATOR = System.getProperty("line.separator");
  private static final byte DELETED = 0;
  private static final byte ACTIVE = 1;
  private final ByteBuffer buffer = ByteBuffer.allocate(4096);
  private final ByteBufferOutput output = new ByteBufferOutput(buffer);
  private final ByteBufferInput input = new ByteBufferInput(buffer);
  private File logFile;
  private RandomAccessFile file;
  private boolean isOpen;
  private long firstIndex;
  private long lastIndex;

  public FileLog(File baseFile) {
    super(baseFile, CopycatEntry.class);
  }

  public FileLog(String baseName) {
    this(new File(baseName));
  }

  @Override
  public synchronized long appendEntry(Entry entry) {
    Assert.isNotNull(entry, "entry");
    assertIsOpen();

    try {
      long index = lastIndex + 1;
      file.writeLong(index);
      file.write(ACTIVE);
      kryo.writeClassAndObject(output, entry);
      byte[] bytes = output.toBytes();
      file.write(bytes);
      file.writeBytes(SEPARATOR);
      output.clear();
      lastIndex = index;
      if (firstIndex == 0) {
        firstIndex = 1;
      }
      return index;
    } catch (IOException e) {
      throw new LogException(e, "Failed to append entry");
    }
  }

  @Override
  public void close() throws IOException {
    assertIsOpen();

    try {
      file.close();
    } finally {
      isOpen = false;
    }
  }

  @Override
  public synchronized void compact(long index, Entry entry) throws IOException {
    Assert.isNotNull(entry, "entry");
    assertIsOpen();

    if (indexInRange(index) && index > firstIndex) {
      // Create a new log file using the most recent timestamp.
      File newLogFile = createLogFile();
      File oldLogFile = logFile;

      // Create a new random access file for reads/writes.
      RandomAccessFile newFile = new RandomAccessFile(newLogFile.getAbsolutePath(), "rw");
      RandomAccessFile oldFile = this.file;

      // Write the snapshot entry as the first entry in the new log.
      newFile.writeLong(index);
      newFile.write(ACTIVE);
      kryo.writeClassAndObject(output, entry);
      byte[] bytes = output.toBytes();
      newFile.write(bytes);
      newFile.writeBytes(SEPARATOR);
      output.clear();

      // Find existing entries with indices greater than the given index and append them to the new
      // log.
      long pointer = findFilePointer(index);
      String line;
      while ((line = file.readLine()) != null) {
        ByteBuffer buffer = ByteBuffer.wrap(line.getBytes());
        long lineIndex = buffer.getLong();
        byte status = buffer.get();
        if (status == ACTIVE) {
          newFile.writeBytes(line);
          newFile.writeBytes(SEPARATOR);
        }
      }
      file.seek(file.getFilePointer());

      // Assign the new log files to log members.
      this.logFile = newLogFile;
      this.file = newFile;

      // Close the old log.
      oldFile.close();
    }
  }

  @Override
  public synchronized boolean containsEntry(long index) {
    assertIsOpen();
    return indexInRange(index);
  }

  @Override
  public void delete() throws IOException {
    logFile.delete();
  }

  @Override
  public synchronized <T extends Entry> T firstEntry() {
    assertIsOpen();
    return getEntry(firstIndex);
  }

  @Override
  public long firstIndex() {
    return firstIndex;
  }

  @Override
  public <T extends Entry> List<T> getEntries(long from, long to) {
    assertIsOpen();
    return null;
  }

  @Override
  public <T extends Entry> T getEntry(long index) {
    assertIsOpen();
    return null;
  }

  @Override
  public boolean isEmpty() {
    assertIsOpen();
    return firstIndex == 0;
  }

  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public synchronized <T extends Entry> T lastEntry() {
    assertIsOpen();
    return getEntry(lastIndex);
  }

  @Override
  public long lastIndex() {
    assertIsOpen();
    return lastIndex;
  }

  @Override
  public void open() throws IOException {
    assertIsNotOpen();
    logFile = findLogFile();

    if (!logFile.exists()) {
      try {
        File parent = logFile.getParentFile();
        if (parent != null) {
          parent.mkdirs();
        }
        logFile.createNewFile();
      } catch (IOException e) {
        throw new LogException(e, "Failed to create file %s", logFile);
      }
    }

    try {
      file = new RandomAccessFile(logFile.getAbsolutePath(), "rw");
      isOpen = true;
    } catch (FileNotFoundException e) {
      throw new LogException(e, "Failed to open file %s", logFile);
    }

    String line = null;
    try {
      String firstLine = file.readLine();
      if (firstLine != null) {
        firstIndex = ByteBuffer.wrap(firstLine.getBytes()).getLong();
      }
      String lastLine = firstLine;
      while ((line = file.readLine()) != null) {
        lastLine = line;
      }

      if (lastLine != null) {
        lastIndex = ByteBuffer.wrap(lastLine.getBytes()).getLong();
      }
    } catch (IOException e) {
      throw new LogException(e, "Failed to read file %s", file);
    }
  }

  @Override
  public synchronized void removeAfter(long index) {
    assertIsOpen();
    long pointer = findFilePointer(index);
    try {
      file.seek(pointer + 8);
      String line;
      while ((line = file.readLine()) != null) {
        file.seek(file.getFilePointer() + 8);
      }
    } catch (IOException e) {
      throw new LogException(e);
    } finally {
      try {
        file.seek(file.length());
      } catch (IOException e) {
      }
    }
  }

  @Override
  public synchronized void removeEntry(long index) {
    assertIsOpen();
    long pointer = findFilePointer(index);
    try {
      file.seek(pointer + 8);
      file.write(DELETED);
    } catch (IOException e) {
      throw new LogException(e);
    } finally {
      try {
        file.seek(file.length());
      } catch (IOException e) {
      }
    }
  }

  @Override
  public long size() {
    assertIsOpen();
    return lastIndex - firstIndex + 1;
  }

  @Override
  public void sync() throws IOException {
    assertIsOpen();
    file.getFD().sync();
  }

  /**
   * Finds the file pointer for the entry at the given index.
   */
  private long findFilePointer(long index) {
    if (!indexInRange(index)) {
      throw new IndexOutOfBoundsException("Index out of bounds");
    }

    try {
      file.seek(0);
      long lastPointer = 0;
      long currentIndex = firstIndex;
      String line = file.readLine();
      do {
        ByteBuffer buffer = ByteBuffer.wrap(file.readLine().getBytes());
        long matchIndex = buffer.getLong();
        byte status = buffer.get();
        if (status == ACTIVE) {
          currentIndex = matchIndex;
        }
        lastPointer = file.getFilePointer();
      } while (currentIndex <= index && (line = file.readLine()) != null);
      file.seek(lastPointer);
      return lastPointer;
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  /**
   * Returns a boolean indicating whether the given index is within the range of the log.
   */
  private boolean indexInRange(long index) {
    return index >= firstIndex && index <= lastIndex;
  }
}
