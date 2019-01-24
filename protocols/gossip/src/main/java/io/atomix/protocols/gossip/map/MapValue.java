/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.protocols.gossip.map;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.atomix.utils.time.Timestamp;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Representation of a value in EventuallyConsistentMap.
 */
public class MapValue implements Comparable<MapValue> {
  private final Timestamp timestamp;
  private final byte[] value;
  private long creationTime;

  /**
   * Creates a tombstone value with the specified timestamp.
   *
   * @param timestamp timestamp for tombstone
   * @return tombstone MapValue
   */
  public static MapValue tombstone(Timestamp timestamp) {
    return new MapValue(null, timestamp, System.currentTimeMillis());
  }

  /**
   * Constructor automatically to create the system time of construction.
   *
   * @param value value
   * @param timestamp value timestamp
   */
  public MapValue(byte[] value, Timestamp timestamp) {
    this(value, timestamp, System.currentTimeMillis());
  }

  /**
   * Creates a map value using value, timestamp, and creation time.
   *
   * @param value value
   * @param timestamp value timestamp.
   * @param creationTime the system time (on local instance) of construction
   */
  public MapValue(byte[] value, Timestamp timestamp, long creationTime) {
    this.value = value;
    this.timestamp = checkNotNull(timestamp, "Timestamp cannot be null");
    this.creationTime = creationTime;
  }

  /**
   * Creates a copy of MapValue.
   * <p>
   * The copy will have an updated creation time corresponding to when the copy was constructed.
   *
   * @return MapValue copy
   */
  public MapValue copy() {
    return new MapValue(this.value, this.timestamp, System.currentTimeMillis());
  }

  /**
   * Tests if this value is tombstone value with the specified timestamp.
   *
   * @return true if this value is null, otherwise false
   */
  public boolean isTombstone() {
    return value == null;
  }

  /**
   * Tests if this value is alive.
   *
   * @return true if this value is not null, otherwise false
   */
  public boolean isAlive() {
    return value != null;
  }

  /**
   * Returns the timestamp of this value.
   *
   * @return timestamp
   */
  public Timestamp timestamp() {
    return timestamp;
  }

  /**
   * Returns this value.
   *
   * @return value
   */
  public byte[] get() {
    return value;
  }

  /**
   * Returns the decoded value.
   *
   * @param decoder the decoder with which to decode the value
   * @param <V> the decoded value type
   * @return the decoded value
   */
  public <V> V get(Function<byte[], V> decoder) {
    return decoder.apply(value);
  }

  /**
   * Returns the creation time of this value.
   *
   * @return creationTime
   */
  public long creationTime() {
    return creationTime;
  }

  /**
   * Tests if this value is newer than the specified MapValue.
   *
   * @param other the value to be compared
   * @return true if this value is newer than other
   */
  public boolean isNewerThan(MapValue other) {
    if (other == null) {
      return true;
    }
    return this.timestamp.isNewerThan(other.timestamp);
  }

  /**
   * Tests if this timestamp is newer than the specified timestamp.
   *
   * @param timestamp timestamp to be compared
   * @return true if this instance is newer
   */
  public boolean isNewerThan(Timestamp timestamp) {
    return this.timestamp.isNewerThan(timestamp);
  }

  /**
   * Returns summary of a MapValue for use during Anti-Entropy exchanges.
   *
   * @return Digest with timestamp and whether this value is null or not
   */
  public Digest digest() {
    return new Digest(timestamp, isTombstone());
  }

  @Override
  public int compareTo(MapValue o) {
    return this.timestamp.compareTo(o.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(timestamp, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object other) {
    if (other instanceof MapValue) {
      MapValue that = (MapValue) other;
      return Objects.equal(this.timestamp, that.timestamp)
          && Objects.equal(this.value, that.value);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("timestamp", timestamp)
        .add("value", value)
        .toString();
  }

  @SuppressWarnings("unused")
  private MapValue() {
    this.timestamp = null;
    this.value = null;
  }

  /**
   * Digest or summary of a MapValue for use during Anti-Entropy exchanges.
   */
  public static class Digest {
    private final Timestamp timestamp;
    private final boolean isTombstone;

    public Digest(Timestamp timestamp, boolean isTombstone) {
      this.timestamp = timestamp;
      this.isTombstone = isTombstone;
    }

    public Timestamp timestamp() {
      return timestamp;
    }

    public boolean isTombstone() {
      return isTombstone;
    }

    public boolean isNewerThan(Digest other) {
      return timestamp.isNewerThan(other.timestamp);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(timestamp, isTombstone);
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof Digest) {
        Digest that = (Digest) other;
        return Objects.equal(this.timestamp, that.timestamp)
            && Objects.equal(this.isTombstone, that.isTombstone);
      }
      return false;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("timestamp", timestamp)
          .add("isTombstone", isTombstone)
          .toString();
    }
  }
}
