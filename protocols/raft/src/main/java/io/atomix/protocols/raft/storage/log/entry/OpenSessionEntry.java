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
package io.atomix.protocols.raft.storage.log.entry;

import io.atomix.protocols.raft.cluster.MemberId;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Open session entry.
 */
public class OpenSessionEntry extends TimestampedEntry {
  private final MemberId member;
  private final String name;
  private final String type;
  private final long timeout;

  public OpenSessionEntry(long term, long timestamp, MemberId member, String name, String type, long timeout) {
    super(term, timestamp);
    this.member = member;
    this.name = name;
    this.type = type;
    this.timeout = timeout;
  }

  /**
   * Returns the client node identifier.
   *
   * @return The client node identifier.
   */
  public MemberId member() {
    return member;
  }

  /**
   * Returns the session state machine name.
   *
   * @return The session's state machine name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the session state machine type name.
   *
   * @return The session's state machine type name.
   */
  public String type() {
    return type;
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  public long timeout() {
    return timeout;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("timestamp", timestamp)
        .add("node", member)
        .add("name", name)
        .add("type", type)
        .add("timeout", timeout)
        .toString();
  }
}
