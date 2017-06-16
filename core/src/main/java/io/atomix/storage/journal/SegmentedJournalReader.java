/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.storage.journal;

import java.util.concurrent.locks.Lock;

/**
 * Segmented journal reader.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SegmentedJournalReader<E> implements JournalReader<E> {
  private final SegmentedJournal<E> journal;
  private final Lock lock;
  private JournalSegment<E> currentSegment;
  private JournalSegmentReader<E> currentReader;

  public SegmentedJournalReader(SegmentedJournal<E> journal, Lock lock, long index) {
    this.journal = journal;
    this.lock = lock;
    initialize(index);
  }

  /**
   * Initializes the reader to the given index.
   */
  private void initialize(long index) {
    currentSegment = journal.segment(index);
    currentReader = currentSegment.createReader();
    long nextIndex = nextIndex();
    while (index > nextIndex && hasNext()) {
      next();
      nextIndex = nextIndex();
    }
  }

  @Override
  public Lock lock() {
    return lock;
  }

  @Override
  public long currentIndex() {
    return currentReader.currentIndex();
  }

  @Override
  public Indexed<E> currentEntry() {
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
  public Indexed<E> get(long index) {
    return reset(index);
  }

  @Override
  public Indexed<E> reset(long index) {
    if (index < currentReader.firstIndex()) {
      currentSegment = journal.previousSegment(currentSegment.index());
      while (currentSegment != null) {
        currentReader.close();
        currentReader = currentSegment.createReader();
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
    currentSegment = journal.firstSegment();
    currentReader = currentSegment.createReader();
  }

  @Override
  public boolean hasNext() {
    if (!currentReader.hasNext()) {
      JournalSegment nextSegment = journal.nextSegment(currentSegment.index());
      if (nextSegment != null) {
        currentSegment = nextSegment;
        currentReader = currentSegment.createReader();
      }
    }
    return currentReader.hasNext();
  }

  @Override
  public Indexed<E> next() {
    return currentReader.next();
  }

  @Override
  public void close() {
    currentReader.close();
  }
}