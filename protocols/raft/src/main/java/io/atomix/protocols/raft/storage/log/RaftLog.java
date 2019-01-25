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
package io.atomix.protocols.raft.storage.log;

import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.journal.DelegatingJournal;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.utils.serializer.Namespace;

import java.io.File;

/**
 * Raft log.
 */
public class RaftLog extends DelegatingJournal<RaftLogEntry> {

  /**
   * Returns a new Raft log builder.
   *
   * @return A new Raft log builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final SegmentedJournal<RaftLogEntry> journal;
  private final boolean flushOnCommit;
  private final RaftLogWriter writer;
  private volatile long commitIndex;

  protected RaftLog(SegmentedJournal<RaftLogEntry> journal, boolean flushOnCommit) {
    super(journal);
    this.journal = journal;
    this.flushOnCommit = flushOnCommit;
    this.writer = new RaftLogWriter(journal.writer(), this);
  }

  @Override
  public RaftLogWriter writer() {
    return writer;
  }

  @Override
  public RaftLogReader openReader(long index) {
    return openReader(index, RaftLogReader.Mode.ALL);
  }

  @Override
  public RaftLogReader openReader(long index, RaftLogReader.Mode mode) {
    return new RaftLogReader(journal.openReader(index), this, mode);
  }

  /**
   * Returns whether {@code flushOnCommit} is enabled for the log.
   *
   * @return Indicates whether {@code flushOnCommit} is enabled for the log.
   */
  boolean isFlushOnCommit() {
    return flushOnCommit;
  }

  /**
   * Commits entries up to the given index.
   *
   * @param index The index up to which to commit entries.
   */
  void setCommitIndex(long index) {
    this.commitIndex = index;
  }

  /**
   * Returns the Raft log commit index.
   *
   * @return The Raft log commit index.
   */
  long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Returns a boolean indicating whether a segment can be removed from the journal prior to the given index.
   *
   * @param index the index from which to remove segments
   * @return indicates whether a segment can be removed from the journal
   */
  public boolean isCompactable(long index) {
    return journal.isCompactable(index);
  }

  /**
   * Returns the index of the last segment in the log.
   *
   * @param index the compaction index
   * @return the starting index of the last segment in the log
   */
  public long getCompactableIndex(long index) {
    return journal.getCompactableIndex(index);
  }

  /**
   * Compacts the journal up to the given index.
   * <p>
   * The semantics of compaction are not specified by this interface.
   *
   * @param index The index up to which to compact the journal.
   */
  public void compact(long index) {
    journal.compact(index);
  }

  /**
   * Raft log builder.
   */
  public static class Builder implements io.atomix.utils.Builder<RaftLog> {
    private static final boolean DEFAULT_FLUSH_ON_COMMIT = false;

    private final SegmentedJournal.Builder<RaftLogEntry> journalBuilder = SegmentedJournal.builder();
    private boolean flushOnCommit = DEFAULT_FLUSH_ON_COMMIT;

    protected Builder() {
    }

    /**
     * Sets the storage name.
     *
     * @param name The storage name.
     * @return The storage builder.
     */
    public Builder withName(String name) {
      journalBuilder.withName(name);
      return this;
    }

    /**
     * Sets the log storage level, returning the builder for method chaining.
     * <p>
     * The storage level indicates how individual entries should be persisted in the journal.
     *
     * @param storageLevel The log storage level.
     * @return The storage builder.
     */
    public Builder withStorageLevel(StorageLevel storageLevel) {
      journalBuilder.withStorageLevel(storageLevel);
      return this;
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(String directory) {
      journalBuilder.withDirectory(directory);
      return this;
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(File directory) {
      journalBuilder.withDirectory(directory);
      return this;
    }

    /**
     * Sets the log serialization namespace, returning the builder for method chaining.
     *
     * @param namespace The journal namespace.
     * @return The journal builder.
     */
    public Builder withNamespace(Namespace namespace) {
      journalBuilder.withNamespace(namespace);
      return this;
    }

    /**
     * Sets the maximum segment size in bytes, returning the builder for method chaining.
     * <p>
     * The maximum segment size dictates when logs should roll over to new segments. As entries are written to
     * a segment of the log, once the size of the segment surpasses the configured maximum segment size, the
     * log will create a new segment and append new entries to that segment.
     * <p>
     * By default, the maximum segment size is {@code 1024 * 1024 * 32}.
     *
     * @param maxSegmentSize The maximum segment size in bytes.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxSegmentSize} is not positive
     */
    public Builder withMaxSegmentSize(int maxSegmentSize) {
      journalBuilder.withMaxSegmentSize(maxSegmentSize);
      return this;
    }

    /**
     * Sets the maximum entry size in bytes, returning the builder for method chaining.
     *
     * @param maxEntrySize the maximum entry size in bytes
     * @return the storage builder
     * @throws IllegalArgumentException if the {@code maxEntrySize} is not positive
     */
    public Builder withMaxEntrySize(int maxEntrySize) {
      journalBuilder.withMaxEntrySize(maxEntrySize);
      return this;
    }

    /**
     * Sets the maximum number of allows entries per segment, returning the builder for method chaining.
     * <p>
     * The maximum entry count dictates when logs should roll over to new segments. As entries are written to
     * a segment of the log, if the entry count in that segment meets the configured maximum entry count, the
     * log will create a new segment and append new entries to that segment.
     * <p>
     * By default, the maximum entries per segment is {@code 1024 * 1024}.
     *
     * @param maxEntriesPerSegment The maximum number of entries allowed per segment.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxEntriesPerSegment} not greater than the default max entries per
     *                                  segment
     * @deprecated since 3.0.2
     */
    @Deprecated
    public Builder withMaxEntriesPerSegment(int maxEntriesPerSegment) {
      journalBuilder.withMaxEntriesPerSegment(maxEntriesPerSegment);
      return this;
    }

    /**
     * Sets the log index density.
     * <p>
     * The index density is the frequency at which the position of entries written to the journal will be recorded in
     * an in-memory index for faster seeking.
     *
     * @param indexDensity the index density
     * @return the log builder
     * @throws IllegalArgumentException if the density is not between 0 and 1
     */
    public Builder withIndexDensity(double indexDensity) {
      journalBuilder.withIndexDensity(indexDensity);
      return this;
    }

    /**
     * Sets the log cache size.
     *
     * @param cacheSize the log cache size
     * @return the log builder
     * @throws IllegalArgumentException if the cache size is not positive
     */
    public Builder withCacheSize(int cacheSize) {
      journalBuilder.withCacheSize(cacheSize);
      return this;
    }

    /**
     * Enables flushing buffers to disk when entries are committed to a segment, returning the builder
     * for method chaining.
     * <p>
     * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time
     * an entry is committed in a given segment.
     *
     * @return The storage builder.
     */
    public Builder withFlushOnCommit() {
      return withFlushOnCommit(true);
    }

    /**
     * Sets whether to flush buffers to disk when entries are committed to a segment, returning the builder
     * for method chaining.
     * <p>
     * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time
     * an entry is committed in a given segment.
     *
     * @param flushOnCommit Whether to flush buffers to disk when entries are committed to a segment.
     * @return The storage builder.
     */
    public Builder withFlushOnCommit(boolean flushOnCommit) {
      this.flushOnCommit = flushOnCommit;
      return this;
    }

    @Override
    public RaftLog build() {
      return new RaftLog(journalBuilder.build(), flushOnCommit);
    }
  }
}
