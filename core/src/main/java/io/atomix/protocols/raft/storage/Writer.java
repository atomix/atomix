/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.storage;

import io.atomix.protocols.raft.storage.entry.Entry;

/**
 * Log writer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Writer extends AutoCloseable {

  /**
   * Returns the last written index.
   *
   * @return The last written index.
   */
  long lastIndex();

  /**
   * Returns the last entry written.
   *
   * @return The last entry written.
   */
  Indexed<? extends Entry<?>> lastEntry();

  /**
   * Returns the next index to be written.
   *
   * @return The next index to be written.
   */
  long nextIndex();

  /**
   * Appends an indexed entry to the log.
   *
   * @param entry The indexed entry to append.
   * @param <T>   The entry type.
   * @return The appended indexed entry.
   */
  <T extends Entry<T>> Indexed<T> append(Indexed<T> entry);

  /**
   * Appends an entry to the writer.
   *
   * @param term  The term in which to append the entry.
   * @param entry The entry to append.
   * @param <T>   The entry type.
   * @return The indexed entry.
   */
  <T extends Entry<T>> Indexed<T> append(long term, T entry);

  /**
   * Truncates the log to the given index.
   *
   * @param index The index to which to truncate the log.
   * @return The updated writer.
   */
  Writer truncate(long index);

  /**
   * Flushes written entries to disk.
   *
   * @return The flushed writer.
   */
  Writer flush();

  @Override
  void close();
}