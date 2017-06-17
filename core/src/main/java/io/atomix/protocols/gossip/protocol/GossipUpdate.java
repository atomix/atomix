/*
 * Copyright 2017-present Open Networking Laboratory
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

import io.atomix.time.Version;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Gossip update message.
 */
public class GossipUpdate<K, V> {
  private final K subject;
  private final V value;
  private final Version version;
  private final boolean tombstone;

  public GossipUpdate(K subject, V value, Version version, boolean tombstone) {
    this.subject = subject;
    this.value = value;
    this.version = version;
    this.tombstone = tombstone;
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
   * Returns the update version.
   *
   * @return the version for the update
   */
  public Version version() {
    return version;
  }

  /**
   * Returns whether the update is a tombstone.
   *
   * @return whether the update is a tombstone
   */
  public boolean isTombstone() {
    return tombstone;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("subject", subject)
        .add("value", value)
        .add("version", version)
        .add("tombstone", tombstone)
        .toString();
  }
}
