/*
 * Copyright 2017-present Open Networking Foundation
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

import java.util.NoSuchElementException;

/**
 * Segmented journal reader.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SegmentedJournalReader<E> implements JournalReader<E> {
  private final SegmentedJournal<E> journal;
  private JournalSegment<E> currentSegment;
  private Indexed<E> previousEntry;
  private JournalSegmentReader<E> currentReader;

  public SegmentedJournalReader(SegmentedJournal<E> journal, long index) {
    this.journal = journal;
    initialize(index);
  }

  /**
   * Initializes the reader to the given index.
   */
  private void initialize(long index) {
    currentSegment = journal.getSegment(index);
    currentReader = currentSegment.createReader();
    long nextIndex = getNextIndex();
    while (index > nextIndex && hasNext()) {
      next();
      nextIndex = getNextIndex();
    }
  }

  /**
   * Returns the first index in the journal.
   *
   * @return the first index in the journal
   */
  public long getFirstIndex() {
    return journal.getFirstSegment().index();
  }

  @Override
  public long getCurrentIndex() {
    long currentIndex = currentReader.getCurrentIndex();
    if (currentIndex != 0) {
      return currentIndex;
    }
    if (previousEntry != null) {
      return previousEntry.index();
    }
    return 0;
  }

  @Override
  public Indexed<E> getCurrentEntry() {
    Indexed<E> currentEntry = currentReader.getCurrentEntry();
    if (currentEntry != null) {
      return currentEntry;
    }
    return previousEntry;
  }

  @Override
  public long getNextIndex() {
    return currentReader.getNextIndex();
  }

  @Override
  public void reset() {
    currentReader.close();
    currentSegment = journal.getFirstSegment();
    currentReader = currentSegment.createReader();
    previousEntry = null;
  }

  @Override
  public void reset(long index) {
    // If the current segment is not open, it has been replaced. Reset the segments.
    if (!currentSegment.isOpen()) {
      reset();
    }

    if (index < currentReader.getNextIndex()) {
      rewind(index);
    } else if (index > currentReader.getNextIndex()) {
      forward(index);
    } else {
      currentReader.reset(index);
    }
  }

  /**
   * Rewinds the journal to the given index.
   */
  private void rewind(long index) {
    if (currentSegment.index() >= index) {
      JournalSegment<E> segment = journal.getSegment(index - 1);
      if (segment != null) {
        currentReader.close();
        currentSegment = segment;
        currentReader = currentSegment.createReader();
      }
    }

    currentReader.reset(index);
    previousEntry = currentReader.getCurrentEntry();
  }

  /**
   * Fast forwards the journal to the given index.
   */
  private void forward(long index) {
    while (getNextIndex() < index && hasNext()) {
      next();
    }
  }

  @Override
  public boolean hasNext() {
    if (!currentReader.hasNext()) {
      JournalSegment<E> nextSegment = journal.getNextSegment(currentSegment.index());
      if (nextSegment != null && nextSegment.index() == getNextIndex()) {
        previousEntry = currentReader.getCurrentEntry();
        currentSegment = nextSegment;
        currentReader = currentSegment.createReader();
        return currentReader.hasNext();
      }
      return false;
    }
    return true;
  }

  @Override
  public Indexed<E> next() {
    if (!currentReader.hasNext()) {
      JournalSegment<E> nextSegment = journal.getNextSegment(currentSegment.index());
      if (nextSegment != null && nextSegment.index() == getNextIndex()) {
        previousEntry = currentReader.getCurrentEntry();
        currentSegment = nextSegment;
        currentReader = currentSegment.createReader();
        return currentReader.next();
      } else {
        throw new NoSuchElementException();
      }
    } else {
      previousEntry = currentReader.getCurrentEntry();
      return currentReader.next();
    }
  }

  @Override
  public void close() {
    currentReader.close();
    journal.closeReader(this);
  }
}