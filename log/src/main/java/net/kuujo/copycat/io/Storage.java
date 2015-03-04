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
package net.kuujo.copycat.io;

/**
 * Byte storage.
 * <p>
 * This is the primary interface for managing the persistence layer that underlies {@link net.kuujo.copycat.io.Block}
 * and {@link net.kuujo.copycat.io.Buffer}. Storage implementations serve to manage references to either in-memory or
 * on-disk storage. The storage abstraction provides access to underlying persistence via fixed-size blocks. This allows
 * memory and disk space to be dynamically allocated according to the needs of higher-level systems.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Storage extends AutoCloseable {

  /**
   * Acquires a reference to the block for the given block index.
   * <p>
   * Although a block may already exist when this method is called, block instances returned by this method will
   * be unique wrappers around the existing block instance. This means that the respective block buffer {@code position}
   * and {@code limit} will be unique among all instances of the block.
   *
   * @param index The index of the block to acquire.
   * @return The acquired block.
   */
  Block acquire(int index);

}
