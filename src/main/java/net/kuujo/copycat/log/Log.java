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

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * A replicated log.
 * 
 * @author Jordan Halterman
 */
public interface Log {

  /**
   * Initializes the log.
   * 
   * @param visitor A log visitor.
   * @param doneHandler A handler to be called once the log is initialized.
   */
  void init(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Sets the maximum log size.
   *
   * @param maxSize The maximum log size.
   * @return The log instance.
   */
  Log setMaxSize(long maxSize);

  /**
   * Returns the maximum log size.
   *
   * @return The maximum log size.
   */
  long getMaxSize();

  /**
   * Sets a full handler on the log.
   *
   * @param handler A handler to be called when the log is full.
   * @return The log instance.
   */
  Log fullHandler(Handler<Void> handler);

  /**
   * Sets a drain handler on the log.
   *
   * @param handler A handler to be called when the log is drained.
   * @return The log instance.
   */
  Log drainHandler(Handler<Void> handler);

  /**
   * Appends an entry to the log.
   * 
   * @param entry The entry to append.
   * @param doneHandler A handler to be called once the entry has been appended.
   * @return The log instance.
   */
  Log appendEntry(Entry entry, Handler<AsyncResult<Long>> doneHandler);

  /**
   * Returns a boolean indicating whether the log has an entry at the given
   * index.
   * 
   * @param index The index to check.
   * @param containsHandler A handler to be called with the contains result.
   * @return Indicates whether the log has an entry at the given index.
   */
  Log containsEntry(long index, Handler<AsyncResult<Boolean>> containsHandler);

  /**
   * Returns the entry at the given index.
   * 
   * @param index The index from which to get the entry.
   * @param entryHandler A handler to be called with the entry.
   * @return The log instance.
   */
  Log getEntry(long index, Handler<AsyncResult<Entry>> entryHandler);

  /**
   * Returns the first log index.
   *
   * @return
   *   The first log index.
   */
  long firstIndex();

  /**
   * Returns the first log entry term.
   *
   * @param doneHandler A handler to be called with the term.
   * @return The log instance.
   */
  Log firstTerm(Handler<AsyncResult<Long>> doneHandler);

  /**
   * Returns the first log entry.
   *
   * @param doneHandler A handler to be called with the entry.
   * @return The log instance.
   */
  Log firstEntry(Handler<AsyncResult<Entry>> doneHandler);

  /**
   * Returns the last log index.
   *
   * @return
   *   The last log index.
   */
  long lastIndex();

  /**
   * Returns the last log entry term.
   *
   * @param doneHandler A handler to be called with the term.
   * @return The log instance.
   */
  Log lastTerm(Handler<AsyncResult<Long>> doneHandler);

  /**
   * Returns the last log entry.
   *
   * @param doneHandler A handler to be called with the entry.
   * @return The log instance.
   */
  Log lastEntry(Handler<AsyncResult<Entry>> doneHandler);

  /**
   * Returns a list of log entries between two given indexes.
   * 
   * @param start The starting index.
   * @param end The ending index.
   * @return A list of entries between the two given indexes.
   */
  Log getEntries(long start, long end, Handler<AsyncResult<List<Entry>>> doneHandler);

  /**
   * Removes the entry at the given index.
   * 
   * @param index The index from which to remove an entry.
   * @return Indicates whether the entry was removed.
   */
  Log removeEntry(long index, Handler<AsyncResult<Entry>> doneHandler);

  /**
   * Removes all entries before the given index.
   * 
   * @param index The index before which to remove entries.
   * @return The log instance.
   */
  Log removeBefore(long index, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Removes all entries after the given index.
   * 
   * @param index The index after which to remove entries.
   * @return The log instance.
   */
  Log removeAfter(long index, Handler<AsyncResult<Void>> doneHandler);

}
