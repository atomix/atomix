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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogException;

import org.vertx.java.core.Handler;

/**
 * A file-based log implementation.
 *
 * @author Jordan Halterman
 */
public class FileLog implements Log {
  private static final long DEFAULT_MAX_SIZE = 32 * 1024 * 1024; // Default 32MB log.
  private File f;
  private RandomAccessFile file;
  private Handler<Void> fullHandler;
  private Handler<Void> drainHandler;
  private boolean full;
  private long firstIndex;
  private long lastIndex;
  private long maxSize = DEFAULT_MAX_SIZE;

  public void open(String filename) {
    f = new File(filename);
    if (!f.exists()) {
      try {
        File parent = f.getParentFile();
        if (parent != null) {
          parent.mkdirs();
        }
        f.createNewFile();
      }
      catch (IOException e) {
        throw new LogException(e);
      }
    }

    try {
      file = new RandomAccessFile(filename, "rw");
    }
    catch (FileNotFoundException e) {
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
    }
    catch (IOException e) {
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
  public <T> long appendEntry(T entry) {
    long index = lastIndex+1;
    try {
      file.writeBytes(String.format("%d:%s%n", index, entry));
      lastIndex++;
      if (firstIndex == 0) {
        firstIndex = 1;
      }
      checkFull();
      return index;
    }
    catch (IOException e) {
      throw new LogException(e);
    }
  }

  @Override
  public boolean containsEntry(long index) {
    return firstIndex <= index && index <= lastIndex;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getEntry(long index) {
    T entry = null;
    try {
      file.seek(0);
      long currentIndex = firstIndex;
      while (currentIndex < index) {
        file.readLine();
        currentIndex++;
      }
      String line = file.readLine();
      if (line != null) {
        entry = (T) line.substring(String.valueOf(index).length() + 1);
      }
      file.seek(file.length());
    }
    catch (IOException e) {
      throw new LogException(e);
    }
    return entry;
  }

  @Override
  public long firstIndex() {
    return firstIndex;
  }

  @Override
  public <T> T firstEntry() {
    return getEntry(firstIndex);
  }

  @Override
  public long lastIndex() {
    return lastIndex;
  }

  @Override
  public <T> T lastEntry() {
    return getEntry(lastIndex);
  }

  @Override
  public <T> List<T> getEntries(long start, long end) {
    List<T> entries = new ArrayList<>();
    for (long i = start; i <= end; i++) {
      T entry = getEntry(i);
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

    try {
      file.seek(0);

      long currentIndex = firstIndex;
      long start = 0;
      while (file.readLine() != null) {
        if (currentIndex >= index-1) {
          start = file.getFilePointer();
          break;
        }
        currentIndex++;
      }

      if (start > 0) {
        long length = file.length();
        int section = 512;
        long gap = 0;
        while (start+gap < length) {
          file.seek(start+gap);
          int readLength = (int) (start+gap+section > length ? length-(start+gap) : section);
          byte[] readBytes = new byte[readLength];
          file.read(readBytes, 0, readLength);
          file.seek(gap);
          file.write(readBytes);
          gap += section;
        }
        file.setLength(file.getFilePointer());
        file.seek(file.length());
      }
      firstIndex = index;
    }
    catch (IOException e) {
      throw new LogException(e);
    }
    checkFull();
  }

  @Override
  public void removeAfter(long index) {
    if (firstIndex == 0) {
      return;
    }

    try {
      file.seek(0);
      long currentIndex = firstIndex;

      if (index < firstIndex) {
        file.setLength(0);
      }

      while (file.readLine() != null) {
        if (currentIndex >= index) {
          file.setLength(file.getFilePointer());
          break;
        }
        currentIndex++;
      }
      lastIndex = index;
      file.seek(file.length());
    }
    catch (IOException e) {
      throw new LogException(e);
    }
    checkFull();
  }

  @Override
  public void close() {
    try {
      if (file != null) {
        file.close();
      }
    }
    catch (IOException e) {
      throw new LogException(e);
    }
  }

  @Override
  public void delete() {
    try {
      file.close();
    }
    catch (IOException e) {
      throw new LogException(e);
    }
    finally {
      if (f != null) {
        f.delete();
      }
    }
  }

  /**
   * Checks whether the log is full.
   */
  private void checkFull() {
    if (!full) {
      if (lastIndex-firstIndex >= maxSize) {
        full = true;
        if (fullHandler != null) {
          fullHandler.handle((Void) null);
        }
      }
    }
    else {
      if (lastIndex-firstIndex < maxSize) {
        full = false;
        if (drainHandler != null) {
          drainHandler.handle((Void) null);
        }
      }
    }
  }

}
