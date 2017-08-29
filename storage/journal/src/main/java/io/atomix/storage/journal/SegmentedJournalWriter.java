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

/**
 * Log writer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SegmentedJournalWriter<E> implements JournalWriter<E> {
  private final SegmentedJournal<E> journal;
  private JournalSegment<E> currentSegment;
  private JournalSegmentWriter<E> currentWriter;

  public SegmentedJournalWriter(SegmentedJournal<E> journal) {
    this.journal = journal;
    this.currentSegment = journal.getLastSegment();
    this.currentWriter = currentSegment.writer();
  }

  @Override
  public long getLastIndex() {
    return currentWriter.getLastIndex();
  }

  @Override
  public Indexed<E> getLastEntry() {
    return currentWriter.getLastEntry();
  }

  @Override
  public long getNextIndex() {
    return currentWriter.getNextIndex();
  }

  /**
   * Resets the head of the journal to the given index.
   *
   * @param index the index to which to reset the head of the journal
   */
  public void reset(long index) {
    currentWriter.close();
    currentSegment = journal.resetSegments(index);
    currentWriter = currentSegment.writer();
    journal.resetHead(index);
  }

  @Override
  public <T extends E> Indexed<T> append(T entry) {
    if (currentWriter.isFull()) {
      currentSegment = journal.getNextSegment();
      currentWriter = currentSegment.writer();
    }
    return currentWriter.append(entry);
  }

  @Override
  public void append(Indexed<E> entry) {
    if (currentWriter.isFull()) {
      currentSegment = journal.getNextSegment();
      currentWriter = currentSegment.writer();
    }
    currentWriter.append(entry);
  }

  @Override
  public void truncate(long index) {
    // Delete all segments with first indexes greater than the given index.
    while (index < currentWriter.firstIndex() - 1) {
      currentWriter.close();
      journal.removeSegment(currentSegment);
      currentSegment = journal.getLastSegment();
      currentWriter = currentSegment.writer();
    }

    // Truncate the current index.
    currentWriter.truncate(index);

    // Reset segment readers.
    journal.resetTail(index + 1);
  }

  @Override
  public void flush() {
    currentWriter.flush();
  }

  @Override
  public void close() {
    currentWriter.close();
  }
}