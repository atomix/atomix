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
package net.kuujo.raft.log;

import java.util.List;

/**
 * A replicated log.
 *
 * @author Jordan Halterman
 */
public interface Log {

  /**
   * Initializes the log.
   */
  void init();

  /**
   * Appends an entry to the log.
   *
   * @param entry
   *   The entry to append.
   * @return
   *   Indicates whether the entry was successfully appended.
   */
  long appendEntry(Entry entry);

  /**
   * Returns a boolean indicating whether the log has an entry at the given index.
   *
   * @param index
   *   The index to check.
   * @return
   *   Indicates whether the log has an entry at the given index.
   */
  boolean containsEntry(long index);

  /**
   * Returns the entry at the given index.
   *
   * @param index
   *   The index from which to get the entry.
   * @return
   *   The entry at the given index.
   */
  Entry entry(long index);

  /**
   * Returns the first index in the log.
   *
   * @return
   *   The first index in the log.
   */
  long firstIndex();

  /**
   * Returns the term of the first entry in the log.
   *
   * @return
   *   The term of the first entry in the log.
   */
  long firstTerm();

  /**
   * Returns the first entry in the log.
   *
   * @return
   *   The first log entry.
   */
  Entry firstEntry();

  /**
   * Returns the last index in the log.
   *
   * @return
   *   The last index in the log.
   */
  long lastIndex();

  /**
   * Returns the term of the last index in the log.
   *
   * @return
   *   The term of the last index in the log.
   */
  long lastTerm();

  /**
   * Returns the last entry in the log.
   *
   * @return
   *   The last log entry.
   */
  Entry lastEntry();

  /**
   * Returns a list of log entries between two given indexes.
   *
   * @param start
   *   The starting index.
   * @param end
   *   The ending index.
   * @return
   *   A list of entries between the two given indexes.
   */
  List<Entry> entries(long start, long end);

  /**
   * Removes the entry at the given index.
   *
   * @param index
   *   The index from which to remove an entry.
   * @return
   *   Indicates whether the entry was removed.
   */
  Entry removeEntry(long index);

  /**
   * Removes all entries before the given index.
   *
   * @param index
   *   The index before which to remove entries.
   * @return
   *   The log instance.
   */
  Log removeBefore(long index);

  /**
   * Removes all entries after the given index.
   *
   * @param index
   *   The index after which to remove entries.
   * @return
   *   The log instance.
   */
  Log removeAfter(long index);

  /**
   * Returns the log floor.
   *
   * @return
   *   The log floor.
   */
  long floor();

  /**
   * Sets the log floor.
   *
   * @param index
   *   The lowest required index in the log.
   * @return
   *   The log instance.
   */
  Log floor(long index);

  /**
   * Frees a command from the log.
   *
   * @param command
   *   The command ID.
   */
  void free(String command);

}
