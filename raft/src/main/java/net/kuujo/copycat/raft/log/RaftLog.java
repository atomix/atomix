/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft.log;

import net.kuujo.copycat.raft.log.entry.RaftEntry;

/**
 * Raft log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RaftLog extends AutoCloseable {

  /**
   * Opens the log.
   */
  void open();

  /**
   * Returns a boolean value indicating whether the log is open.
   *
   * @return Indicates whether the log is open.
   */
  boolean isOpen();

  /**
   * Returns a boolean value indicating whether the log is empty.
   *
   * @return Indicates whether the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  boolean isEmpty();

  /**
   * Returns the size of the log on disk in bytes.
   *
   * @return The size of the log in bytes.
   */
  long size();

  /**
   * Returns the number of entries in the log.
   * <p>
   * The length is the number of physical entries on disk. Note, however, that the length of the log may actually differ
   * from the number of entries eligible for reads due to deduplication.
   *
   * @return The number of entries in the log.
   */
  long length();

  /**
   * Returns the log's current first index.
   * <p>
   * If no entries have been written to the log then the first index will be {@code 0}. If the log contains entries then
   * the first index will be {@code 1}.
   *
   * @return The index of the first entry in the log or {@code 0} if the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  long firstIndex();

  /**
   * Returns the index of the last entry in the log.
   * <p>
   * If no entries have been written to the log then the last index will be {@code 0}.
   *
   * @return The index of the last entry in the log or {@code 0} if the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  long lastIndex();

  /**
   * Creates a new log entry.
   * <p>
   * Users should ensure that the returned {@link net.kuujo.copycat.raft.log.entry.RaftEntry} is closed once the write is complete. Closing the entry will
   * result in its contents being persisted to the log. Only a single {@link net.kuujo.copycat.raft.log.entry.RaftEntry} instance may be open via the
   * this method at any given time.
   *
   * @param type The entry type.
   * @return The log entry.
   * @throws IllegalStateException If the log is not open
   * @throws NullPointerException If the entry type is {@code null}
   */
  <T extends RaftEntry<T>> T createEntry(Class<T> type);

  /**
   * Appends an entry to the log.
   *
   * @param entry The entry to append.
   * @return The appended entry index.
   * @throws java.lang.NullPointerException If the entry is {@code null}
   * @throws CommitModificationException If the entry's index does not match
   *         the expected next log index.
   */
  long appendEntry(RaftEntry entry);

  /**
   * Gets an entry from the log at the given index.
   * <p>
   * If the given index is outside of the bounds of the log then a {@link IndexOutOfBoundsException} will be
   * thrown. If the entry at the given index has been compacted from the log due to an entry with the same key at a
   * higher index then the returned entry will be {@code null}.
   * <p>
   * Entries returned by this method are pooled and {@link net.kuujo.copycat.io.util.ReferenceCounted}. In order to ensure
   * the entry is released back to the internal entry pool call {@link RaftEntry#close()} or load the entry in a
   * try-with-resources statement.
   * <pre>
   *   {@code
   *   try (RaftEntry entry = log.getEntry(123)) {
   *     // Do some stuff...
   *   }
   *   }
   * </pre>
   *
   * @param index The index of the entry to get.
   * @return The entry at the given index or {@code null} if the entry doesn't exist.
   * @throws IllegalStateException If the log is not open.
   * @throws IndexOutOfBoundsException If the given index is not within the bounds of the log.
   */
  <T extends RaftEntry<T>> T getEntry(long index);

  /**
   * Returns a boolean value indicating whether the given index is within the bounds of the log.
   * <p>
   * If the index is less than {@code 1} or greater than {@link RaftLog#lastIndex()} then this method will return
   * {@code false}, otherwise {@code true}.
   *
   * @param index The index to check.
   * @return Indicates whether the given index is within the bounds of the log.
   * @throws IllegalStateException If the log is not open.
   */
  boolean containsIndex(long index);

  /**
   * Returns a boolean value indicating whether the log contains a live entry at the given index.
   * <p>
   * An entry is considered <i>live</i> if it is the highest entry in the log with its key. If the entry at the provided
   * {@code index} has been superseded by an entry with the same key at a higher index then this method <i>may</i>
   * return {@code false}. However, because of the semantics of the log compaction algorithm, it is not guaranteed that
   * two entries with the same key will not exist in the log at the same time.
   * <p>
   * Note that only entries that have been {@link RaftLog#commit(long) committed} are considered for compaction. Therefore,
   * entries where {@link RaftEntry#getIndex()} ()} is greater than the highest committed index will be live.
   *
   * @param index The index to check.
   * @return Indicates whether the log contains a live entry at the given index.
   * @throws IllegalStateException If the log is not open.
   */
  boolean containsEntry(long index);

  /**
   * Skips the given number of entries.
   * <p>
   * This method essentially advances the log's {@link RaftLog#lastIndex()} without writing any entries at the interim
   * indices. Note that calling {@code Loggable#truncate()} after {@code skip()} will result in the skipped entries
   * being partially or completely reverted.
   *
   * @param entries The number of entries to skip.
   * @return The log.
   * @throws IllegalStateException If the log is not open.
   * @throws IllegalArgumentException If the number of entries is less than {@code 1}
   * @throws IndexOutOfBoundsException If skipping the given number of entries places the index out of the bounds of the log.
   */
  RaftLog skip(long entries);

  /**
   * Truncates the log up to the given index.
   * <p>
   * If the given {@code index} is less than or equal to the log's {@code commitIndex} then a
   * {@link CommitModificationException} will be thrown. Otherwise, entries after the given index will be permanently
   * removed from the log, and the log's {@link RaftLog#lastIndex()} will become equal to {@code index}. If entries
   * have been {@link RaftLog#skip(long) skipped} then the skipped entries will be truncated as well.
   *
   * @param index The index at which to truncate the log.
   * @return The updated log.
   * @throws IllegalStateException If the log is not open.
   * @throws CommitModificationException If first index after the given index has already been committed.
   * @throws IndexOutOfBoundsException If the given index is not within the bounds of the log.
   */
  RaftLog truncate(long index);

  /**
   * Sets a filter on the log.
   * <p>
   * During log compaction, entries can be optionally filtered out of the log by explicitly evaluating individual entries.
   *
   * @param filter The entry filter.
   * @return The Raft log.
   */
  RaftLog filter(RaftEntryFilter filter);

  /**
   * Commits entries up to the given index.
   * <p>
   * The given commit {@code index} must be greater than the previous commit index. When entries are
   * committed to the log they are made available for compaction. Entries that have been committed may not be removed
   * via {@link RaftLog#truncate(long)}, but prior entries with the same keys will be deduplicated and effectively
   * removed from the view of the user via compaction.
   * <p>
   * Note that the commit index is <i>not</i> persistent. In the event of a failure and recovery, the
   * commit index will be reset to {@code 0}.
   *
   * @param index The index up to which to commit entries.
   * @throws IllegalStateException If the log is not open.
   * @throws IndexOutOfBoundsException If the index is not within the bounds of the log.
   */
  void commit(long index);

  /**
   * Flushes the log to disk.
   *
   * @throws IllegalStateException If the log is not open.
   */
  void flush();

  /**
   * Closes the log.
   */
  @Override
  void close();

  /**
   * Returns a boolean value indicating whether the log is closed.
   *
   * @return Indicates whether the log is closed.
   */
  boolean isClosed();

  /**
   * Deletes the log.
   */
  void delete();

  /**
   * Raft log builder.
   */
  static interface Builder extends net.kuujo.copycat.Builder<RaftLog> {
  }

}
