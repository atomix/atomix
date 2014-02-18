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

import net.kuujo.copycat.log.Entry;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * An asynchronous log.
 *
 * @author Jordan Halterman
 */
public interface AsyncLog {

  /**
   * Sets the log file name.
   *
   * @param filename The log file name.
   * @return The log proxy.
   */
  public AsyncLog setLogFile(String filename);

  /**
   * Returns the log file name.
   *
   * @return The log file name.
   */
  public String getLogFile();

  /**
   * Sets the maximum log size.
   *
   * @param maxSize The maximum log size.
   * @return The log proxy.
   */
  public AsyncLog setMaxSize(long maxSize);

  /**
   * Returns the maximum log size.
   *
   * @return The maximum log size.
   */
  public long getMaxSize();

  /**
   * Opens the log.
   *
   * @param doneHandler An asynchronous handler to be called once the
   *                    log has been opened.
   */
  public void open(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Closes the log.
   *
   * @param doneHandler An asynchronous handler to be called once the
   *                    log has closed.
   */
  public void close(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Deletes the log.
   *
   * @param doneHandler An asynchronous handler to be called once the
   *                    log has been removed.
   */
  public void delete(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Sets a handler to be calle when the log is full.
   *
   * @param handler A handler to be called when the log is full.
   * @return The log proxy.
   */
  public AsyncLog fullHandler(Handler<Void> handler);

  /**
   * Sets a handler to be calle when the log is drained.
   *
   * @param handler A handler to be called when the log is drained.
   * @return The log proxy.
   */
  public AsyncLog drainHandler(Handler<Void> handler);

  /**
   * Appends an entry to the log.
   * 
   * @param entry The entry to append.
   * @param doneHandler A handler to be called once the entry has been appended.
   * @return The log instance.
   */
  public AsyncLog appendEntry(Entry entry, Handler<AsyncResult<Long>> doneHandler);

  /**
   * Returns a boolean indicating whether the log has an entry at the given
   * index.
   * 
   * @param index The index to check.
   * @param containsHandler A handler to be called with the contains result.
   * @return Indicates whether the log has an entry at the given index.
   */
  public AsyncLog containsEntry(long index, Handler<AsyncResult<Boolean>> containsHandler);

  /**
   * Returns the entry at the given index.
   * 
   * @param index The index from which to get the entry.
   * @param entryHandler A handler to be called with the entry.
   * @return The log instance.
   */
  public AsyncLog getEntry(long index, Handler<AsyncResult<Entry>> entryHandler);

  /**
   * Returns the first log index.
   *
   * @return
   *   The first log index.
   */
  public AsyncLog firstIndex(Handler<AsyncResult<Long>> resultHandler);

  /**
   * Returns the first log entry term.
   *
   * @param doneHandler A handler to be called with the term.
   * @return The log instance.
   */
  public AsyncLog firstTerm(Handler<AsyncResult<Long>> doneHandler);

  /**
   * Returns the first log entry.
   *
   * @param doneHandler A handler to be called with the entry.
   * @return The log instance.
   */
  public AsyncLog firstEntry(Handler<AsyncResult<Entry>> doneHandler);

  /**
   * Returns the last log index.
   *
   * @return
   *   The last log index.
   */
  public AsyncLog lastIndex(Handler<AsyncResult<Long>> resultHandler);

  /**
   * Returns the last log entry term.
   *
   * @param doneHandler A handler to be called with the term.
   * @return The log instance.
   */
  public AsyncLog lastTerm(Handler<AsyncResult<Long>> doneHandler);

  /**
   * Returns the last log entry.
   *
   * @param doneHandler A handler to be called with the entry.
   * @return The log instance.
   */
  public AsyncLog lastEntry(Handler<AsyncResult<Entry>> doneHandler);

  /**
   * Returns a list of log entries between two given indexes.
   * 
   * @param start The starting index.
   * @param end The ending index.
   * @return A list of entries between the two given indexes.
   */
  public AsyncLog getEntries(long start, long end, Handler<AsyncResult<List<Entry>>> doneHandler);

  /**
   * Removes all entries before the given index.
   * 
   * @param index The index before which to remove entries.
   * @return The log instance.
   */
  public AsyncLog removeBefore(long index, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Removes all entries after the given index.
   * 
   * @param index The index after which to remove entries.
   * @return The log instance.
   */
  public AsyncLog removeAfter(long index, Handler<AsyncResult<Void>> doneHandler);

}
