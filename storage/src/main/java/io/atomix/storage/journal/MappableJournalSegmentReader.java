// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import io.atomix.storage.StorageException;
import io.atomix.storage.journal.index.JournalIndex;
import io.atomix.utils.serializer.Namespace;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Mappable log segment reader.
 */
class MappableJournalSegmentReader<E> implements JournalReader<E> {
  private final JournalSegment<E> segment;
  private final FileChannel channel;
  private final int maxEntrySize;
  private final JournalIndex index;
  private final Namespace namespace;
  private JournalReader<E> reader;

  MappableJournalSegmentReader(
      FileChannel channel,
      JournalSegment<E> segment,
      int maxEntrySize,
      JournalIndex index,
      Namespace namespace) {
    this.channel = channel;
    this.segment = segment;
    this.maxEntrySize = maxEntrySize;
    this.index = index;
    this.namespace = namespace;
    this.reader = new FileChannelJournalSegmentReader<>(channel, segment, maxEntrySize, index, namespace);
  }

  /**
   * Converts the reader to a mapped reader using the given buffer.
   *
   * @param buffer the mapped buffer
   */
  void map(ByteBuffer buffer) {
    if (!(reader instanceof MappedJournalSegmentReader)) {
      JournalReader<E> reader = this.reader;
      this.reader = new MappedJournalSegmentReader<>(buffer, segment, maxEntrySize, index, namespace);
      this.reader.reset(reader.getNextIndex());
      reader.close();
    }
  }

  /**
   * Converts the reader to an unmapped reader.
   */
  void unmap() {
    if (reader instanceof MappedJournalSegmentReader) {
      JournalReader<E> reader = this.reader;
      this.reader = new FileChannelJournalSegmentReader<>(channel, segment, maxEntrySize, index, namespace);
      this.reader.reset(reader.getNextIndex());
      reader.close();
    }
  }

  @Override
  public long getFirstIndex() {
    return reader.getFirstIndex();
  }

  @Override
  public long getCurrentIndex() {
    return reader.getCurrentIndex();
  }

  @Override
  public Indexed<E> getCurrentEntry() {
    return reader.getCurrentEntry();
  }

  @Override
  public long getNextIndex() {
    return reader.getNextIndex();
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public Indexed<E> next() {
    return reader.next();
  }

  @Override
  public void reset() {
    reader.reset();
  }

  @Override
  public void reset(long index) {
    reader.reset(index);
  }

  @Override
  public void close() {
    reader.close();
    try {
      channel.close();
    } catch (IOException e) {
      throw new StorageException(e);
    } finally {
      segment.closeReader(this);
    }
  }
}
