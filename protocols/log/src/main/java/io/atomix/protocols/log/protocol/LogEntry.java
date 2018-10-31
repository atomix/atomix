/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.protocols.log.protocol;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Log entry.
 */
public class LogEntry {
  private final long term;
  private final long timestamp;
  private final byte[] value;

  public LogEntry(long term, long timestamp, byte[] value) {
    this.term = term;
    this.timestamp = timestamp;
    this.value = value;
  }

  public long term() {
    return term;
  }

  public long timestamp() {
    return timestamp;
  }

  public byte[] value() {
    return value;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term())
        .add("timestamp", timestamp())
        .add("value", ArraySizeHashPrinter.of(value))
        .toString();
  }
}
