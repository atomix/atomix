/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.gossip;

import io.atomix.event.Event;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Gossip event.
 */
public class GossipEvent<K, V> implements Event<GossipEvent.Type, K> {

  /**
   * Gossip event type.
   */
  public enum Type {
    UPDATE,
  }

  private final long time;
  private final K subject;
  private final V value;

  public GossipEvent(K subject, V value) {
    this(System.currentTimeMillis(), subject, value);
  }

  public GossipEvent(long time, K subject, V value) {
    this.time = time;
    this.subject = subject;
    this.value = value;
  }

  @Override
  public long time() {
    return time;
  }

  @Override
  public Type type() {
    return Type.UPDATE;
  }

  @Override
  public K subject() {
    return subject;
  }

  /**
   * Returns the event value.
   *
   * @return The event value.
   */
  public V value() {
    return value;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("time", time)
        .add("subject", subject)
        .add("value", value)
        .toString();
  }
}
