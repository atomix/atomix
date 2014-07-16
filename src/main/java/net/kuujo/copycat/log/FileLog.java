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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import net.kuujo.copycat.util.serializer.Serializer;

import org.vertx.java.core.Handler;

/**
 * A file-based log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileLog implements Log {
  private static final Serializer serializer  = Serializer.getInstance();
  private static final long DEFAULT_MAX_SIZE = 32 * 1024 * 1024; // Default 32MB log.
  private File f;
  private RandomAccessFile file;
  private Handler<Void> fullHandler;
  private Handler<Void> drainHandler;
  private boolean full;
  private long firstIndex;
  private long lastIndex;
  private long maxSize = DEFAULT_MAX_SIZE;

  public FileLog(String fileName) {
    this.f = new File(fileName);
  }

  public FileLog(File file) {
    this.f = file;
  }

  @Override
  public void open() {
    if (!f.exists()) {
      try {
        File parent = f.getParentFile();
        if (parent != null) {
          parent.mkdirs();
        }
        f.createNewFile();
      } catch (IOException e) {
        throw new LogException(e);
      }
    }

    try {
      file = new RandomAccessFile(f.getAbsolutePath(), "rw");
    } catch (FileNotFoundException e) {
      throw new LogException(e);
    }

    String line = null;
    try {
      String firstLine = file.readLine();
      if (firstLine != null) {
        firstIndex = parseIndex(firstLine);
      }
      String lastLine = firstLine;
      while ((line = file.readLine()) != null) {
        lastLine = line;
      }

      if (lastLine != null) {
        lastIndex = parseIndex(lastLine);
      }
    } catch (IOException e) {
      throw new LogException(e);
    }
    checkFull();
  }

  private long parseIndex(String line) {
    return Long.valueOf(line.substring(0, line.indexOf(":")));
  }

  @Override
  public Log setMaxSize(long maxSize) {
    this.maxSize = maxSize;
    return null;
  }

  @Override
  public long getMaxSize() {
    return maxSize;
  }

  @Override
  public Log fullHandler(Handler<Void> handler) {
    fullHandler = handler;
    return this;
  }

  @Override
  public Log drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  @Override
  public long appendEntry(Entry entry) {
    long index = lastIndex+1;
    try {
      file.writeBytes(String.format("%d:%s%n", index, serializer.writeString(entry)));
      lastIndex++;
      if (firstIndex == 0) {
        firstIndex = 1;
      }
      checkFull();
      return index;
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  @Override
  public boolean containsEntry(long index) {
    return firstIndex <= index && index <= lastIndex;
  }

  @Override
  public Entry getEntry(long index) {
    Entry entry = null;
    try {
      findFilePointer(index);
      String line = file.readLine();
      if (line != null) {
        entry = serializer.readString(line.substring(String.valueOf(index).length() + 1), Entry.class);
      }
      file.seek(file.length());
    } catch (IOException e) {
      throw new LogException(e);
    }
    return entry;
  }

  @Override
  public Log setEntry(long index, Entry entry) {
    if (index < firstIndex || index > lastIndex) throw new IndexOutOfBoundsException();
    try {
      long pointer = findFilePointer(index);
      String line = file.readLine();
      int length = line.length();
      String bytes = String.format("%d:%s%n", index, serializer.writeString(entry));
      int newLength = bytes.length();
      if (newLength > length) {
        expandFile(pointer+length, pointer+newLength);
      } else if (length > newLength) {
        compactFile(pointer+newLength, pointer+length);
      }
      file.seek(pointer);
      file.writeBytes(bytes);
      file.seek(file.length());
    } catch (IOException e) {
      throw new LogException(e);
    }
    return this;
  }

  @Override
  public long firstIndex() {
    return firstIndex;
  }

  @Override
  public Entry firstEntry() {
    return getEntry(firstIndex);
  }

  @Override
  public long lastIndex() {
    return lastIndex;
  }

  @Override
  public Entry lastEntry() {
    return getEntry(lastIndex);
  }

  @Override
  public List<Entry> getEntries(long start, long end) {
    List<Entry> entries = new ArrayList<>();
    for (long i = start; i <= end; i++) {
      Entry entry = getEntry(i);
      if (entry != null) {
        entries.add(entry);
      }
    }
    return entries;
  }

  @Override
  public void removeBefore(long index) {
    if (index > lastIndex) {
      return;
    }

    long pointer = findFilePointer(index);
    compactFile(pointer, 0);
    firstIndex = index;
    full = false;
    checkFull();
  }

  @Override
  public void removeAfter(long index) {
    if (firstIndex == 0) {
      return;
    }

    try {
      long pointer = findFilePointer(index+1);
      file.setLength(pointer);
      lastIndex = index;
      file.seek(file.length());
    } catch (IOException e) {
      throw new LogException(e);
    }
    checkFull();
  }

  /**
   * Finds the file pointer for the entry at the given index.
   */
  private long findFilePointer(long index) {
    try {
      file.seek(0);
      long currentIndex = firstIndex;
      while (currentIndex < index) {
        file.readLine();
        currentIndex++;
      }
      return file.getFilePointer();
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  /**
   * Compacts the file by rewriting later bytes over earlier bytes.
   */
  private void compactFile(long from, long to) {
    if (to > from) throw new IllegalArgumentException("Cannont compact file from " + from + " to " + to);
    try {
      long originalLength = file.length();
      long readCursor = from;
      long writeCursor = to;
      int bufferSize = 4096;
      while (readCursor < originalLength) {
        int bytesToRead = (int) (readCursor + bufferSize > originalLength ? originalLength - readCursor : bufferSize);
        file.seek(readCursor);
        byte[] bytes = new byte[bytesToRead];
        file.read(bytes, 0, bytesToRead);
        readCursor += bytesToRead;
        file.seek(writeCursor);
        file.write(bytes);
        writeCursor += bytesToRead;
      }
      file.setLength(originalLength - (from - to));
      file.seek(file.length());
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  /**
   * Extends the length of the file by moving bytes from the start position to the end position.
   */
  private void expandFile(long from, long to) {
    if (from > to) throw new IllegalArgumentException("Cannot expand file from " + from + " to " + to);
    try {
      long difference = to - from;
      long originalLength = file.length();
      long newLength = originalLength + difference;
      file.setLength(newLength);
      long readCursor = originalLength;
      long writeCursor = newLength;
      int bufferSize = 4096;
      while (readCursor > from) {
        int bytesToRead = (int) (readCursor - bufferSize < from ? readCursor - from : bufferSize);
        readCursor -= bytesToRead;
        file.seek(readCursor);
        byte[] bytes = new byte[bytesToRead];
        file.read(bytes, 0, bytesToRead);
        writeCursor -= bytesToRead;
        file.seek(writeCursor);
        file.write(bytes);
      }
      file.seek(file.length());
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  @Override
  public void close() {
    try {
      if (file != null) {
        file.close();
      }
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  @Override
  public void delete() {
    try {
      file.close();
    } catch (IOException e) {
      throw new LogException(e);
    } finally {
      if (f != null) {
        f.delete();
      }
    }
  }

  /**
   * Checks whether the log is full.
   */
  private void checkFull() {
    long length;
    try {
      length = file.length();
    } catch (IOException e) {
      return;
    }

    if (!full) {
      if (length >= maxSize) {
        full = true;
        if (fullHandler != null) {
          fullHandler.handle((Void) null);
        }
      }
    } else {
      if (length < maxSize) {
        full = false;
        if (drainHandler != null) {
          drainHandler.handle((Void) null);
        }
      }
    }
  }

}
