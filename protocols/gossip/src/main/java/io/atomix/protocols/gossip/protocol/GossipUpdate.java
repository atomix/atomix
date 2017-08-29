/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.gossip.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.atomix.time.Timestamp;
import io.atomix.time.Version;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Gossip update message.
 */
public class GossipUpdate<K, V> {
  private final K subject;
  private final V value;
  private final Timestamp timestamp;
  private final transient long creationTime = System.currentTimeMillis();

  public GossipUpdate(K subject, V value, Timestamp timestamp) {
    this.subject = subject;
    this.value = value;
    this.timestamp = timestamp;
  }

  /**
   * Returns the update subject.
   *
   * @return the update subject
   */
  public K subject() {
    return subject;
  }

  /**
   * Returns the update value.
   *
   * @return the update value
   */
  public V value() {
    return value;
  }

  /**
   * Returns the update timestamp.
   *
   * @return the timestamp for the update
   */
  public Timestamp timestamp() {
    return timestamp;
  }

  /**
   * Returns the update creation time.
   *
   * @return the update creation time
   */
  public long creationTime() {
    return creationTime;
  }

  /**
   * Returns whether the update is a tombstone.
   *
   * @return whether the update is a tombstone
   */
  public boolean isTombstone() {
    return value == null;
  }

  /**
   * Tests if this value is newer than the specified update.
   *
   * @param other the value to be compared
   * @return true if this value is newer than other
   */
  public boolean isNewerThan(GossipUpdate<K, V> other) {
    return other == null || this.timestamp.isNewerThan(other.timestamp);
  }

  /**
   * Tests if this update is newer than the specified timestamp.
   *
   * @param timestamp timestamp to be compared
   * @return true if this instance is newer
   */
  public boolean isNewerThan(Timestamp timestamp) {
    return this.timestamp.isNewerThan(timestamp);
  }

  /**
   * Returns summary of a update for use during anti-entropy exchanges.
   *
   * @return Digest with timestamp and whether this value is null or not
   */
  public Digest digest() {
    return new Digest(timestamp, isTombstone());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("subject", subject)
        .add("value", value)
        .add("timestamp", timestamp)
        .toString();
  }

  /**
   * Gossip update digest.
   */
  public static class Digest {
    private final Timestamp timestamp;
    private final boolean isTombstone;

    public Digest(Timestamp timestamp, boolean isTombstone) {
      this.timestamp = timestamp;
      this.isTombstone = isTombstone;
    }

    /**
     * Returns the update timestamp.
     *
     * @return the update timestamp
     */
    public Timestamp timestamp() {
      return timestamp;
    }

    /**
     * Returns whether the update is a tombstone.
     *
     * @return whether the update is a tombstone
     */
    public boolean isTombstone() {
      return isTombstone;
    }

    /**
     * Returns whether the update is newer than the given update digest.
     *
     * @param other the digest for the update with which to compare the digest
     * @return indicates whether the update associated with this digest is newer than the given update digest
     */
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
        return Objects.equal(this.timestamp, that.timestamp) &&
            Objects.equal(this.isTombstone, that.isTombstone);
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
