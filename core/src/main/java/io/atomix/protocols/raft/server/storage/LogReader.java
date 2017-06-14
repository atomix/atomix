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
package io.atomix.protocols.raft.server.storage;

import io.atomix.protocols.raft.server.storage.entry.Entry;

import java.util.concurrent.locks.Lock;

/**
 * Log reader.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class LogReader implements Reader {
  private final SegmentManager segments;
  private final Lock lock;
  private final Mode mode;
  private Segment currentSegment;
  private SegmentReader currentReader;

  public LogReader(SegmentManager segments, Lock lock, long index, Mode mode) {
    this.segments = segments;
    this.lock = lock;
    this.mode = mode;
    initialize(index);
  }

  /**
   * Initializes the reader to the given index.
   */
  private void initialize(long index) {
    currentSegment = segments.segment(index);
    currentReader = currentSegment.createReader(mode);
    long nextIndex = nextIndex();
    while (index > nextIndex && hasNext()) {
      next();
      nextIndex = nextIndex();
    }
  }

  @Override
  public Mode mode() {
    return mode;
  }

  /**
   * Locks the reader.
   *
   * @return The log reader.
   */
  public Reader lock() {
    lock.lock();
    return this;
  }

  /**
   * Unlocks the reader.
   *
   * @return The log reader.
   */
  public Reader unlock() {
    lock.unlock();
    return this;
  }

  @Override
  public long currentIndex() {
    return currentReader.currentIndex();
  }

  @Override
  public Indexed<? extends Entry<?>> currentEntry() {
    return currentReader.currentEntry();
  }

  @Override
  public long nextIndex() {
    if (hasNext()) {
      return currentReader.nextIndex();
    }
    return -1;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Entry<T>> Indexed<T> get(long index) {
    return (Indexed<T>) reset(index);
  }

  @Override
  public Indexed<? extends Entry<?>> reset(long index) {
    if (index < currentReader.firstIndex()) {
      currentSegment = segments.previousSegment(currentSegment.index());
      while (currentSegment != null) {
        currentReader.close();
        currentReader = currentSegment.createReader(mode);
        if (currentReader.firstIndex() < index) {
          break;
        }
      }
    }
    return currentReader.reset(index);
  }

  @Override
  public void reset() {
    currentReader.close();
    currentSegment = segments.firstSegment();
    currentReader = currentSegment.createReader(mode);
  }

  @Override
  public boolean hasNext() {
    if (!currentReader.hasNext()) {
      Segment nextSegment = segments.nextSegment(currentSegment.index());
      if (nextSegment != null) {
        currentSegment = nextSegment;
        currentReader = currentSegment.createReader(mode);
      }
    }
    return currentReader.hasNext();
  }

  @Override
  public Indexed<? extends Entry<?>> next() {
    return currentReader.next();
  }

  @Override
  public void close() {
    currentReader.close();
  }
}