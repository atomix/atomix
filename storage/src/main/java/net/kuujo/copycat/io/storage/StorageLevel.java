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
 * Log storage level constants.
 * <p>
 * The storage level indicates the persistence mechanism to be used by the Copycat {@link Log}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public enum StorageLevel {

  /**
   * Memory storage level.
   * <p>
   * The memory storage level will configure the {@link Log} to use a {@link net.kuujo.copycat.io.HeapBuffer}
   * to store log data in a {@code byte[]} backed memory buffer.
   */
  MEMORY,

  /**
   * Disk storage level.
   * <p>
   * The disk storage level will configure the {@link Log} to use a {@link net.kuujo.copycat.io.FileBuffer}
   * to store log data on disk in a series of segment files.
   */
  DISK

}
