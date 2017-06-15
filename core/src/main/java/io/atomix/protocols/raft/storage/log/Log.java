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
package io.atomix.protocols.raft.storage.log;

import io.atomix.protocols.raft.storage.Storage;

import java.io.Closeable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Log.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Log implements Closeable {
  private final SegmentManager segments;
  private final LogWriter writer;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private volatile boolean open = true;

  public Log(String name, Storage storage) {
    this.segments = new SegmentManager(name, storage);
    this.writer = new LogWriter(segments, lock.writeLock());
  }

  /**
   * Returns the log writer.
   *
   * @return The log writer.
   */
  public LogWriter writer() {
    return writer;
  }

  /**
   * Creates a new log reader.
   *
   * @param index The index at which to start the reader.
   * @param mode  The mode in which to open the log reader.
   * @return A new log reader.
   */
  public LogReader createReader(long index, Reader.Mode mode) {
    return new LogReader(segments, lock.readLock(), index, mode);
  }

  /**
   * Returns a boolean indicating whether the log is open.
   *
   * @return Indicates whether the log is open.
   */
  public boolean isOpen() {
    return open;
  }

  /**
   * Compacts the log.
   *
   * @param index The index to which to compact the log.
   */
  public void compact(long index) {
    writer.lock();
    try {
      segments.compact(index);
    } finally {
      writer.unlock();
    }
  }

  @Override
  public void close() {
    segments.close();
    open = false;
  }
}