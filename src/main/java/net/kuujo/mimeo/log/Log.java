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
package net.kuujo.mimeo.log;

import java.util.List;

import net.kuujo.mimeo.Command;

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
  void init(LogVisitor visitor, Handler<AsyncResult<Void>> doneHandler);

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
   * @return The entry at the given index.
   */
  Log entry(long index, Handler<AsyncResult<Entry>> entryHandler);

  /**
   * Returns the first index in the log.
   * 
   * @return The first index in the log.
   */
  Log firstIndex(Handler<AsyncResult<Long>> handler);

  /**
   * Returns the term of the first entry in the log.
   * 
   * @return The term of the first entry in the log.
   */
  Log firstTerm(Handler<AsyncResult<Long>> handler);

  /**
   * Returns the first entry in the log.
   * 
   * @return The first log entry.
   */
  Log firstEntry(Handler<AsyncResult<Entry>> handler);

  /**
   * Returns the last index in the log.
   * 
   * @return The last index in the log.
   */
  Log lastIndex(Handler<AsyncResult<Long>> handler);

  /**
   * Returns the term of the last index in the log.
   * 
   * @return The term of the last index in the log.
   */
  Log lastTerm(Handler<AsyncResult<Long>> handler);

  /**
   * Returns the last entry in the log.
   * 
   * @return The last log entry.
   */
  Log lastEntry(Handler<AsyncResult<Entry>> handler);

  /**
   * Returns a list of log entries between two given indexes.
   * 
   * @param start The starting index.
   * @param end The ending index.
   * @return A list of entries between the two given indexes.
   */
  Log entries(long start, long end, Handler<AsyncResult<List<Entry>>> doneHandler);

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

  /**
   * Returns the log floor.
   * 
   * @return The log floor.
   */
  Log floor(Handler<AsyncResult<Long>> doneHandler);

  /**
   * Sets the log floor.
   * 
   * @param index The lowest required index in the log.
   * @return The log instance.
   */
  Log floor(long index, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Frees a command from the log.
   * 
   * @param command The command ID.
   */
  void free(String command);

  /**
   * Frees a command from the log.
   * 
   * @param command The command ID.
   * @param doneHandler A handler to be called once the command is freed.
   */
  void free(String command, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Frees a command from the log.
   * 
   * @param command The command ID.
   */
  @SuppressWarnings("rawtypes")
  void free(Command command);

  /**
   * Frees a command from the log.
   * 
   * @param command The command to free.
   * @param doneHandler A handler to be called once the command is freed.
   */
  @SuppressWarnings("rawtypes")
  void free(Command command, Handler<AsyncResult<Void>> doneHandler);

}
