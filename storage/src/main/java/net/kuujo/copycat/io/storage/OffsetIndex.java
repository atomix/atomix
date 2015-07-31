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
package net.kuujo.copycat.io.storage;

/**
 * Offset index.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
interface OffsetIndex extends AutoCloseable {

  /**
   * Returns the first offset in the index.
   */
  int firstOffset();

  /**
   * Returns the last offset in the index.
   */
  int lastOffset();

  /**
   * Indexes the given offset with the given position.
   *
   * @param offset The offset to index.
   * @param position The position of the offset to index.
   */
  void index(int offset, long position, int length);

  /**
   * Returns the number of entries active in the index.
   *
   * @return The number of entries active in the index.
   */
  int size();

  /**
   * Returns a boolean value indicating whether the index contains the given offset.
   *
   * @param offset The offset to check.
   * @return Indicates whether the index contains the given offset.
   */
  boolean contains(int offset);

  /**
   * Finds the starting position of the given offset.
   *
   * @param offset The offset to look up.
   * @return The starting position of the given offset.
   */
  long position(int offset);

  /**
   * Finds the length of the given offset by locating the next offset in the index.
   *
   * @param offset The offset for which to look up the length.
   * @return The last position of the offset entry.
   */
  int length(int offset);

  /**
   * Truncates the index up to the given offset.
   * <p>
   * This method assumes that the given offset is contained within the index. If the offset is not indexed then the
   * index will not be truncated.
   *
   * @param offset The offset after which to truncate the index.
   */
  void truncate(int offset);

  /**
   * Flushes the index to the underlying storage.
   */
  void flush();

  @Override
  void close();

}
