/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.storage.log.entry;

import io.atomix.utils.misc.TimestampPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Base class for timestamped entries.
 */
public abstract class TimestampedEntry extends RaftLogEntry {
  protected final long timestamp;

  public TimestampedEntry(long term, long timestamp) {
    super(term);
    this.timestamp = timestamp;
  }

  /**
   * Returns the entry timestamp.
   *
   * @return The entry timestamp.
   */
  public long timestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("timestamp", new TimestampPrinter(timestamp))
        .toString();
  }
}
