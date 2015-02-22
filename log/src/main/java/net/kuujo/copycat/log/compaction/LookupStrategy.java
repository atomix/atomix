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
package net.kuujo.copycat.log.compaction;

import java.nio.ByteBuffer;

/**
 * Log key lookup strategy for deduplication.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LookupStrategy {

  /**
   * Puts a key in the table.
   *
   * @param key The key to put into the table.
   * @param index The index of the key.
   */
  void put(ByteBuffer key, long index);

  /**
   * Returns a boolean value indicating whether the lookup table contains the given key.
   *
   * @param key The key for which to search the lookup table.
   * @return Indicates whether the lookup table contains the given key.
   */
  boolean contains(ByteBuffer key);

  /**
   * Gets the index of a key in the table.
   *
   * @param key The key to get.
   * @return The index of the given key or {@code null} if the key does not exist in the table.
   */
  Long get(ByteBuffer key);

}
