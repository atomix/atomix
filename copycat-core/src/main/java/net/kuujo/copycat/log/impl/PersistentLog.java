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
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.kuujo.copycat.log.Buffer;
import net.kuujo.copycat.log.Compactable;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.LogIndexOutOfBoundsException;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.Excerpt;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;

/**
 * Java chronicle based log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PersistentLog extends AbstractLog implements Compactable {
  private static final byte DELETED = 0;
  private static final byte ACTIVE = 1;
  private static final SimpleDateFormat fileNameFormat = new SimpleDateFormat("yyyyMMddhhmmssSSS");
  private final File baseFile;
  private File logFile;
  private Chronicle chronicle;
  private Excerpt excerpt;
  private Buffer excerptBuffer;
  private ExcerptAppender appender;
  private Buffer appenderBuffer;
  private ExcerptTailer tailer;
  private Buffer tailerBuffer;
  private long firstIndex;
  private long lastIndex;

  public PersistentLog(String baseName) {
    this(baseName, Entry.class);
  }

  public PersistentLog(File baseFile) {
    this(baseFile, Entry.class);
  }

  public PersistentLog(String baseName, Class<? extends Entry> entryType) {
    this(new File(baseName), entryType);
  }

  public PersistentLog(File baseFile, Class<? extends Entry> entryType) {
    super(entryType);
    this.baseFile = baseFile;
  }

  @Override
  public void open() throws IOException {
    logFile = findLogFile();
    chronicle = new IndexedChronicle(logFile.getAbsolutePath());

    excerpt = chronicle.createExcerpt();
    excerptBuffer = new PersistentBuffer(excerpt);
    appender = chronicle.createAppender();
    appenderBuffer = new PersistentBuffer(appender);
    tailer = chronicle.createTailer();
    tailerBuffer = new PersistentBuffer(tailer);

    tailer.toStart();
    while (tailer.nextIndex()) {
      long index = tailer.readLong();
      byte deleted = tailer.readByte();
      if (deleted == ACTIVE) {
        if (firstIndex == 0) {
          firstIndex = index;
        }
        lastIndex = index;
      }
    }
  }

  /**
   * Finds the most recent long file.
   */
  private File findLogFile() {
    baseFile.getParentFile().mkdirs();
    File logFile = null;
    long logTime = 0;
    for (File file : baseFile.getParentFile().listFiles(file -> file.isFile())) {
      if (file.getName().substring(0, file.getName().indexOf('.')).equals(baseFile.getName())) {
        try {
          long fileTime = fileNameFormat.parse(file.getName().substring(file.getName().indexOf('.') + 1, file.getName().indexOf('.', file.getName().indexOf('.') + 1))).getTime();
          if (fileTime > logTime) {
            logFile = new File(file.getParentFile().getAbsolutePath(), file.getName().substring(0, file.getName().indexOf('.', file.getName().indexOf('.') + 1)));
            logTime  = fileTime;
          }
        } catch (ParseException e) {
        }
      }
    }

    if (logFile == null) {
      logFile = createLogFile();
    }
    return logFile;
  }

  /**
   * Creates a new log file.
   */
  private File createLogFile() {
    return new File(baseFile.getParentFile().getAbsolutePath(), String.format("%s.%s", baseFile.getName(), fileNameFormat.format(new Date())));
  }

  /**
   * Deletes a log file.
   */
  private void deleteLogFile(File logFile) {
    for (File file : baseFile.getParentFile().listFiles(file -> file.isFile())) {
      if (file.getName().startsWith(logFile.getName())) {
        file.delete();
      }
    }
  }

  /**
   * Returns the next log index.
   *
   * @return The next log index.
   */
  private long nextIndex() {
    long index = ++lastIndex;
    if (firstIndex == 0) {
      firstIndex = 1;
    }
    return index;
  }

  @Override
  public long size() {
    return lastIndex - firstIndex;
  }

  @Override
  public boolean isEmpty() {
    return lastIndex > firstIndex;
  }

  @Override
  @SuppressWarnings("unchecked")
  public long appendEntry(Entry entry) {
    long index = nextIndex();
    appender.startExcerpt();
    appender.writeLong(index);
    appender.writeByte(ACTIVE);
    byte entryType = getEntryType(entry.getClass());
    appender.writeByte(entryType);
    getWriter(entryType).writeEntry(entry, appenderBuffer);
    appender.finish();
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
    return indexInRange(index);
  }

  @Override
  public long firstIndex() {
    return firstIndex;
  }

  @Override
  public <T extends Entry> T firstEntry() {
    return getEntry(firstIndex);
  }

  @Override
  public long lastIndex() {
    return lastIndex;
  }

  @Override
  public <T extends Entry> T lastEntry() {
    return getEntry(lastIndex);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Entry> T getEntry(long index) {
    long matchIndex = findAbsoluteIndex(index);
    excerpt.index(matchIndex);
    excerpt.skip(9);
    byte entryType = excerpt.readByte();
    return (T) getReader(entryType).readEntry(excerptBuffer);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Entry> List<T> getEntries(long from, long to) {
    if (!indexInRange(from)) {
      throw new LogIndexOutOfBoundsException("From index out of bounds.");
    }
    if (!indexInRange(to)) {
      throw new LogIndexOutOfBoundsException("To index out of bounds.");
    }

    List<T> entries = new ArrayList<>();
    long matchIndex = findAbsoluteIndex(from);
    tailer.index(matchIndex);
    tailer.skip(9);
    byte entryType = tailer.readByte();
    entries.add((T) getReader(entryType).readEntry(tailerBuffer));
    while (tailer.nextIndex() && matchIndex < to) {
      long index = tailer.readLong();
      byte status = tailer.readByte();
      if (status == ACTIVE) {
        entryType = tailer.readByte();
        entries.add((T) getReader(entryType).readEntry(tailerBuffer));
        matchIndex = index;
      }
    }
    return entries;
  }

  @Override
  public void removeEntry(long index) {
    if (!indexInRange(index)) {
      throw new LogIndexOutOfBoundsException(String.format("Cannot remove entry at index %d", index));
    }
    long matchIndex = findAbsoluteIndex(index);
    if (matchIndex > -1) {
      tailer.index(matchIndex);
      tailer.skip(8);
      tailer.writeByte(DELETED);
    }
  }

  @Override
  public void removeAfter(long index) {
    if (!indexInRange(index)) {
      throw new LogIndexOutOfBoundsException(String.format("Cannot remove entry at index %d", index));
    }
    long matchIndex = findAbsoluteIndex(index);
    if (matchIndex > -1) {
      tailer.index(matchIndex);
      tailer.skip(8);
      tailer.writeByte(DELETED);
      while (tailer.nextIndex()) {
        tailer.skip(8);
        tailer.writeByte(DELETED);
      }
    }
  }

  /**
   * Finds the absolute index of a log entry in the chronicle by log index.
   */
  private long findAbsoluteIndex(long index) {
    return excerpt.findMatch((excerpt) -> {
      long match = excerpt.readLong();
      if (match < index) {
        return -1;
      } else if (match > index) {
        return 1;
      } else {
        byte status = excerpt.readByte();
        if (status == DELETED) {
          return -1;
        }
      }
      return 0;
    });
  }

  /**
   * Returns a boolean indicating whether the given index is within the range
   * of the log.
   */
  private boolean indexInRange(long index) {
    return index >= firstIndex && index <= lastIndex;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void compact(long index, Entry snapshot) throws IOException {
    if (index > firstIndex) {
      // Create a new log file using the most recent timestamp.
      File newLogFile = createLogFile();
      File oldLogFile = logFile;
  
      // Create a new chronicle for the new log file.
      Chronicle chronicle = new IndexedChronicle(newLogFile.getAbsolutePath());
      ExcerptAppender appender = chronicle.createAppender();
      Buffer appenderBuffer = new PersistentBuffer(appender);
      appender.startExcerpt();
      appender.writeLong(index);
      appender.writeByte(ACTIVE);
      byte type = getEntryType(snapshot.getClass());
      appender.writeByte(type);
      getWriter(type).writeEntry(snapshot, appenderBuffer);
      appender.finish();
  
      // Iterate through entries equal to or greater than the given index and copy them to the new chronicle.
      long matchIndex = findAbsoluteIndex(index-1);
      tailer.index(matchIndex);
      while (tailer.nextIndex()) {
        long entryIndex = tailer.readLong();
        byte entryStatus = tailer.readByte();
        if (entryStatus == ACTIVE) {
          byte entryType = tailer.readByte();
          Entry entry = getReader(entryType).readEntry(tailerBuffer);
          appender.startExcerpt();
          appender.writeLong(entryIndex);
          appender.writeByte(entryStatus);
          appender.writeByte(entryType);
          getWriter(entryType).writeEntry(entry, appenderBuffer);
          appender.finish();
        }
      }
  
      // Override existing chronicle types.
      this.logFile = newLogFile;
      this.chronicle = chronicle;
      this.excerpt = chronicle.createExcerpt();
      this.excerptBuffer = new PersistentBuffer(excerpt);
      this.appender = appender;
      this.appenderBuffer = appenderBuffer;
      this.tailer = chronicle.createTailer();
      this.tailerBuffer = new PersistentBuffer(tailer);
  
      // Finally, delete the old log file.
      deleteLogFile(oldLogFile);
    }
  }

  @Override
  public void close() throws IOException {
    chronicle.close();
    firstIndex = 0;
    lastIndex = 0;
  }

  @Override
  public void delete() {
    chronicle.clear();
  }

}
