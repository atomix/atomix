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
package net.kuujo.copycat.resource;

/**
 * Copycat resource storage level.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public enum StorageLevel {

  /**
   * Stores all state changes in memory.
   */
  MEMORY,

  /**
   * Buffers state changes and periodically flushes state to disk.
   */
  BUFFERED,

  /**
   * Writes all state changes directly to disk.
   */
  DISK

}
