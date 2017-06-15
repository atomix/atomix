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
package io.atomix.protocols.raft.storage;

/**
 * {@link Log} storage level configuration values which control how logs are stored on disk or in memory.
 * <p>
 * Storage levels represent the method used to store the individual {@link Segment segments} that make up a
 * {@link Log}. When configuring a {@link Storage} module, the storage can be configured to write
 * {@link io.atomix.copycat.server.storage.entry.Entry entries} to disk, memory, or memory-mapped files using the
 * values provided by this enum. The {@code StorageLevel} configuration dictates the type of
 * {@link io.atomix.catalyst.buffer.Buffer} to which to write entries. See the specific storage levels for more
 * information.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 * @see Storage
 */
public enum StorageLevel {

  /**
   * Stores logs in memory only.
   * <p>
   * In-memory logs will be written to {@link Segment segments} backed by {@link io.atomix.catalyst.buffer.HeapBuffer}.
   * Each {@code MEMORY} segment may only store up to {@code 2^32-1} bytes. <em>Entries written to in-memory logs are
   * not recoverable after a crash.</em> If your use case requires strong persistence, use {@link #DISK} or {@link #MAPPED}
   * storage.
   */
  MEMORY,

  /**
   * Stores logs in memory mapped files.
   * <p>
   * Memory mapped logs will be written to {@link Segment segments} backed by {@link io.atomix.catalyst.buffer.MappedBuffer}.
   * Entries written to memory mapped files may be recovered after a crash, but the {@code MAPPED} storage level does not
   * guarantee that <em>all</em> entries written to the log will be persisted. Additionally, the use of persistent storage
   * levels reduces the amount of overhead required to catch the log up at startup.
   */
  MAPPED,

  /**
   * Stores logs on disk.
   * <p>
   * On-disk logs will be written to {@link Segment segments} backed by {@link io.atomix.catalyst.buffer.FileBuffer}, which
   * in turn is backed by {@link java.io.RandomAccessFile}. Entries written to {@code DISK} storage can be recovered in the
   * event of a failure or other restart. Additionally, the use of persistent storage levels reduces the amount of overhead
   * required to catch the log up at startup.
   */
  DISK

}
