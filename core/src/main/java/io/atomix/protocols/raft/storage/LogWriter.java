/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.storage;

import io.atomix.protocols.raft.storage.entry.Entry;

import java.util.concurrent.locks.Lock;

/**
 * Log writer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class LogWriter implements Writer {
  private final SegmentManager segments;
  private final Lock lock;
  private volatile Segment currentSegment;
  private volatile SegmentWriter currentWriter;

  public LogWriter(SegmentManager segments, Lock lock) {
    this.segments = segments;
    this.lock = lock;
    this.currentSegment = segments.lastSegment();
    this.currentWriter = currentSegment.writer();
  }

  /**
   * Locks the writer.
   *
   * @return The log writer.
   */
  public LogWriter lock() {
    lock.lock();
    return this;
  }

  /**
   * Unlocks the writer.
   *
   * @return The log writer.
   */
  public LogWriter unlock() {
    lock.unlock();
    return this;
  }

  @Override
  public long lastIndex() {
    return currentWriter.lastIndex();
  }

  @Override
  public Indexed<? extends Entry<?>> lastEntry() {
    return currentWriter.lastEntry();
  }

  @Override
  public long nextIndex() {
    return currentWriter.nextIndex();
  }

  /**
   * Commits entries up to the given index.
   *
   * @param index The index up to which to commit entries.
   * @return The log writer.
   */
  public LogWriter commit(long index) {
    segments.commitIndex(index);
    return this;
  }

  @Override
  public <T extends Entry<T>> Indexed<T> append(Indexed<T> entry) {
    if (currentWriter.isFull()) {
      currentSegment = segments.nextSegment();
      currentWriter = currentSegment.writer();
    }
    return currentWriter.append(entry);
  }

  @Override
  public <T extends Entry<T>> Indexed<T> append(long term, T entry) {
    if (currentWriter.isFull()) {
      currentSegment = segments.nextSegment();
      currentWriter = currentSegment.writer();
    }
    return currentWriter.append(term, entry);
  }

  @Override
  public LogWriter truncate(long index) {
    // Delete all segments with first indexes greater than the given index.
    while (index < currentWriter.firstIndex() - 1) {
      currentWriter.close();
      segments.removeSegment(currentSegment);
      currentSegment = segments.lastSegment();
      currentWriter = currentSegment.writer();
    }

    // Truncate the current index.
    currentWriter.truncate(index);
    return this;
  }

  @Override
  public LogWriter flush() {
    currentWriter.flush();
    return this;
  }

  @Override
  public void close() {
    currentWriter.close();
  }
}