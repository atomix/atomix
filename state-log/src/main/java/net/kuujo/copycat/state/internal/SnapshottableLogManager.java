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
package net.kuujo.copycat.state.internal;

import net.kuujo.copycat.log.LogConfig;
import net.kuujo.copycat.log.LogManager;
import net.kuujo.copycat.log.LogSegment;
import net.kuujo.copycat.util.internal.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Snapshottable log manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SnapshottableLogManager implements LogManager {
  private final LogManager logManager;
  private final LogManager snapshotManager;

  public SnapshottableLogManager(LogManager logManager, LogManager snapshotManager) {
    this.logManager = logManager;
    this.snapshotManager = snapshotManager;
  }

  @Override
  public LogConfig config() {
    return logManager.config();
  }

  @Override
  public TreeMap<Long, LogSegment> segments() {
    return logManager.segments();
  }

  @Override
  public LogSegment segment() {
    return logManager.segment();
  }

  @Override
  public LogSegment segment(long index) {
    return snapshotManager.lastIndex() == null || index > snapshotManager.lastIndex() ? logManager.segment(index) : snapshotManager.segment(index);
  }

  @Override
  public LogSegment firstSegment() {
    return snapshotManager.isEmpty() ? logManager.firstSegment() : snapshotManager.firstSegment();
  }

  @Override
  public LogSegment lastSegment() {
    return logManager.lastSegment();
  }

  @Override
  public void open() throws IOException {
    snapshotManager.open();
    logManager.open();
  }

  @Override
  public boolean isEmpty() {
    return snapshotManager.isEmpty() && logManager.isEmpty();
  }

  @Override
  public boolean isOpen() {
    return snapshotManager.isOpen() && logManager.isOpen();
  }

  @Override
  public long size() {
    return snapshotManager.size() + logManager.size();
  }

  @Override
  public long entryCount() {
    return snapshotManager.entryCount() + logManager.entryCount();
  }

  /**
   * Returns a boolean value indicating whether the given index is a snapshottable index.
   *
   * @param index The index to check.
   * @return Indicates whether a snapshot can be taken at the given index.
   */
  public boolean isSnapshottable(long index) {
    LogSegment segment = logManager.segment(index);
    if (segment == null) {
      return false;
    } else if (segment.lastIndex() == null || segment.lastIndex() != index) {
      return false;
    } else if (segment == logManager.lastSegment()) {
      return false;
    }
    return true;
  }

  /**
   * Appends a snapshot to the log.
   *
   * @param index The index at which to write the snapshot.
   * @param snapshot The snapshot to append to the snapshot log.
   * @return The index at which the snapshot was written.
   * @throws IOException If the log could not be rolled over.
   */
  public long appendSnapshot(long index, List<ByteBuffer> snapshot) throws IOException {
    LogSegment segment = logManager.segment(index);
    if (segment == null) {
      throw new IndexOutOfBoundsException("Invalid snapshot index " + index);
    } else if (segment.lastIndex() != index) {
      throw new IllegalArgumentException("Snapshot index must be the last index of a segment");
    } else if (segment == logManager.lastSegment()) {
      throw new IllegalArgumentException("Cannot snapshot current log segment");
    }

    // When appending a snapshot, force the snapshot log manager to roll over to a new segment, append the snapshot
    // to the log, and then compact the log once the snapshot has been appended.
    snapshotManager.rollOver(index - snapshot.size() + 1);
    for (ByteBuffer entry : snapshot) {
      snapshotManager.appendEntry(entry);
    }
    compact(snapshotManager);
    compact(logManager);
    return index;
  }

  /**
   * Compacts the given log, removing all segments except for the last segment.
   */
  private void compact(LogManager log) {
    for (Iterator<Map.Entry<Long, LogSegment>> iterator = log.segments().entrySet().iterator(); iterator.hasNext(); ) {
      LogSegment segment = iterator.next().getValue();
      if (log.lastSegment() != segment) {
        iterator.remove();
        try {
          segment.close();
          segment.delete();
        } catch (IOException e) {
        }
      }
    }
  }

  @Override
  public long appendEntry(ByteBuffer entry) throws IOException {
    return logManager.appendEntry(entry);
  }

  @Override
  public Long firstIndex() {
    return !snapshotManager.isEmpty() ? snapshotManager.firstIndex() : logManager.firstIndex();
  }

  @Override
  public Long lastIndex() {
    return logManager.lastIndex();
  }

  @Override
  public boolean containsIndex(long index) {
    Assert.state(isOpen(), "Log is not open");
    return logManager.containsIndex(index) || snapshotManager.containsIndex(index);
  }

  @Override
  public ByteBuffer getEntry(long index) {
    Assert.state(isOpen(), "Log is not open");
    if (logManager.containsIndex(index)) {
      return logManager.getEntry(index);
    } else if (snapshotManager.containsIndex(index)) {
      return snapshotManager.getEntry(index);
    }
    throw new IndexOutOfBoundsException("No entry at index " + index);
  }

  @Override
  public void removeAfter(long index) {
    Assert.state(isOpen(), "Log is not open");
    Assert.index(index, logManager.containsIndex(index), "Log index out of bounds");
    logManager.removeAfter(index);
  }

  @Override
  public void rollOver(long index) throws IOException {
    logManager.rollOver(index);
  }

  @Override
  public void compact(long index) throws IOException {
    logManager.compact(index);
    snapshotManager.compact(index);
  }

  @Override
  public void flush() {
    logManager.flush();
  }

  @Override
  public void close() throws IOException {
    logManager.close();
    snapshotManager.close();
  }

  @Override
  public boolean isClosed() {
    return logManager.isClosed();
  }

  @Override
  public void delete() {
    logManager.delete();
    snapshotManager.delete();
  }

}
