/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.log;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Distributed log record.
 */
public class Record<V> {
  private final long offset;
  private final long timestamp;
  private final V value;

  public Record(long offset, long timestamp, V value) {
    this.offset = offset;
    this.timestamp = timestamp;
    this.value = value;
  }

  /**
   * Returns the record offset.
   *
   * @return the record offset
   */
  public long offset() {
    return offset;
  }

  /**
   * Returns the record timestamp.
   *
   * @return the record timestamp
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Returns the record value.
   *
   * @return the record value
   */
  public V value() {
    return value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, timestamp, value);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Record) {
      Record that = (Record) object;
      return this.offset == that.offset && this.timestamp == that.timestamp && this.value == that.value;
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("offset", offset)
        .add("timestamp", timestamp)
        .add("value", value)
        .toString();
  }
}
