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

import io.atomix.time.LogicalTimestamp;

import java.util.Collection;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Base type for gossip messages.
 */
public class GossipMessage<K, V> {
  private final LogicalTimestamp timestamp;
  private final Collection<GossipUpdate<K, V>> updates;

  public GossipMessage(LogicalTimestamp timestamp, Collection<GossipUpdate<K, V>> updates) {
    this.timestamp = timestamp;
    this.updates = updates;
  }

  /**
   * Returns the logical timestamp for the message.
   *
   * @return the logical timestamp for the message
   */
  public LogicalTimestamp timestamp() {
    return timestamp;
  }

  /**
   * Returns a list of gossip updates.
   *
   * @return a list of gossip updates
   */
  public Collection<GossipUpdate<K, V>> updates() {
    return updates;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("timestamp", timestamp)
        .add("updates", updates)
        .toString();
  }
}
