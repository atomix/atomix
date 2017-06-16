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
package io.atomix.protocols.raft.storage.log;

import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.util.serializer.Serializer;

import java.io.File;

/**
 * Raft log.
 */
public class RaftLog extends SegmentedJournal<RaftLogEntry> {

  /**
   * Returns a new Raft log builder.
   *
   * @return A new Raft log builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final boolean flushOnCommit;
  private volatile long commitIndex;

  public RaftLog(
      String name,
      StorageLevel storageLevel,
      File directory,
      Serializer serializer,
      int maxSegmentSize,
      int maxEntriesPerSegment,
      int entryBufferSize,
      boolean flushOnCommit) {
    super(name, storageLevel, directory, serializer, maxSegmentSize, maxEntriesPerSegment, entryBufferSize);
    this.flushOnCommit = flushOnCommit;
  }

  @Override
  protected RaftLogWriter openWriter() {
    return new RaftLogWriter(this, lock.writeLock());
  }

  @Override
  protected RaftLogReader openReader(long index) {
    return openReader(index, RaftLogReader.Mode.ALL);
  }

  /**
   * Opens a new Raft log reader.
   *
   * @param index The reader index.
   * @param mode The reader mode.
   * @return A new Raft log reader.
   */
  protected RaftLogReader openReader(long index, RaftLogReader.Mode mode) {
    return new RaftLogReader(this, lock.readLock(), index, mode);
  }

  @Override
  public RaftLogWriter writer() {
    return (RaftLogWriter) super.writer();
  }

  @Override
  public RaftLogReader createReader(long index) {
    return createReader(index, RaftLogReader.Mode.ALL);
  }

  /**
   * Creates a new Raft log reader with the given reader mode.
   *
   * @param index The index from which to begin reading entries.
   * @param mode The mode in which to read entries.
   * @return The Raft log reader.
   */
  public RaftLogReader createReader(long index, RaftLogReader.Mode mode) {
    return openReader(index, mode);
  }

  /**
   * Returns whether {@code flushOnCommit} is enabled for the log.
   *
   * @return Indicates whether {@code flushOnCommit} is enabled for the log.
   */
  public boolean isFlushOnCommit() {
    return flushOnCommit;
  }

  /**
   * Commits entries up to the given index.
   *
   * @param index The index up to which to commit entries.
   */
  void commitIndex(long index) {
    this.commitIndex = index;
  }

  /**
   * Returns the Raft log commit index.
   *
   * @return The Raft log commit index.
   */
  long commitIndex() {
    return commitIndex;
  }

  /**
   * Raft log builder.
   */
  public static class Builder extends SegmentedJournal.Builder<RaftLogEntry> {
    private static final boolean DEFAULT_FLUSH_ON_COMMIT = false;
    private boolean flushOnCommit = DEFAULT_FLUSH_ON_COMMIT;

    protected Builder() {
    }

    @Override
    public Builder withName(String name) {
      super.withName(name);
      return this;
    }

    @Override
    public Builder withStorageLevel(StorageLevel storageLevel) {
      super.withStorageLevel(storageLevel);
      return this;
    }

    @Override
    public Builder withDirectory(String directory) {
      super.withDirectory(directory);
      return this;
    }

    @Override
    public Builder withDirectory(File directory) {
      super.withDirectory(directory);
      return this;
    }

    @Override
    public Builder withSerializer(Serializer serializer) {
      super.withSerializer(serializer);
      return this;
    }

    @Override
    public Builder withMaxSegmentSize(int maxSegmentSize) {
      super.withMaxSegmentSize(maxSegmentSize);
      return this;
    }

    @Override
    public Builder withMaxEntriesPerSegment(int maxEntriesPerSegment) {
      super.withMaxEntriesPerSegment(maxEntriesPerSegment);
      return this;
    }

    @Override
    public Builder withEntryBufferSize(int entryBufferSize) {
      super.withEntryBufferSize(entryBufferSize);
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
      return new RaftLog(name, storageLevel, directory, serializer, maxSegmentSize, maxEntriesPerSegment, entryBufferSize, flushOnCommit);
    }
  }
}
