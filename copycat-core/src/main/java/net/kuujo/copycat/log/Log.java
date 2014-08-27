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
package net.kuujo.copycat.log;

import java.util.List;

import net.kuujo.copycat.event.EventProvider;

/**
 * Replicated event log.<p>
 *
 * The log is at the core of CopyCat's replication system. Logs are
 * used to replicate cluster configuration information, leader state,
 * and state machine commands. Because CopyCat supports snapshotting
 * logs, the <code>Log</code> interface supports a wide variety of
 * methods for modifying the log size and structure.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Log extends EventProvider<EntryListener> {

  /**
   * Opens the log.
   */
  void open();

  /**
   * Returns the current size of the log.
   *
   * @return The current size of the log.
   */
  long size();

  /**
   * Returns a boolean indicating whether the log is empty.
   *
   * @return Indicates whether the log is empty.
   */
  boolean isEmpty();

  /**
   * Appends an entry to the log.
   * 
   * @param entry The entry to append.
   * @return The index at which the entry was appended.
   */
  long appendEntry(Entry entry);

  /**
   * Appends a list of entries to the log.
   *
   * @param entries The entries to append.
   * @return A list of indices for the appended entries.
   */
  List<Long> appendEntries(Entry... entries);

  /**
   * Appends a list of entries to the log.
   *
   * @param entries The entries to append.
   * @return A list of indices for the appended entries.
   */
  List<Long> appendEntries(List<? extends Entry> entries);

  /**
   * Sets an entry in the log.
   *
   * @param index The index of the entry to set.
   * @param entry The entry to set.
   * @return The entry index.
   */
  long setEntry(long index, Entry entry);

  /**
   * Prepends an entry to the log.
   * 
   * @param entry The entry to prepend.
   * @return The index at which the entry was prepended.
   */
  long prependEntry(Entry entry);

  /**
   * Prepends a list of entries to the log.
   *
   * @param entries The entries to prepend.
   * @return A list of indices for the prepended entries.
   */
  List<Long> prependEntries(Entry... entries);

  /**
   * Prepends a list of entries to the log.
   *
   * @param entries The entries to prepend.
   * @return A list of indices for the prepended entries.
   */
  List<Long> prependEntries(List<? extends Entry> entries);

  /**
   * Returns a boolean indicating whether the log has an entry at the given
   * index.
   * 
   * @param index The index to check.
   * @return Indicates whether the log has an entry at the given index.
   */
  boolean containsEntry(long index);

  /**
   * Returns the entry at the given index.
   * 
   * @param index The index from which to get the entry.
   * @return A log entry.
   */
  Entry getEntry(long index);

  /**
   * Returns the first log index.
   *
   * @return
   *   The first log index.
   */
  long firstIndex();

  /**
   * Returns the first log entry.
   *
   * @return The first log entry.
   */
  Entry firstEntry();

  /**
   * Returns the last log index.
   *
   * @return
   *   The last log index.
   */
  long lastIndex();

  /**
   * Returns the last log entry.
   *
   * @return The last log entry.
   */
  Entry lastEntry();

  /**
   * Returns a list of log entries between two given indexes.
   * 
   * @param start The starting index.
   * @param end The ending index.
   * @return A list of entries between the two given indexes.
   */
  List<Entry> getEntries(long start, long end);

  /**
   * Removes all entries before the given index.
   * 
   * @param index The index before which to remove entries.
   */
  void removeBefore(long index);

  /**
   * Removes all entries after the given index.
   * 
   * @param index The index after which to remove entries.
   */
  void removeAfter(long index);

  /**
   * Backs up the log, creating a separate backup file.
   */
  void backup();

  /**
   * Commits the log, removing the backup.
   */
  void commit();

  /**
   * Restores the log from a separate backup file.
   */
  void restore();

  /**
   * Closes the log.
   */
  void close();

  /**
   * Deletes the log.
   */
  void delete();

}
