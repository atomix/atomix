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
package io.atomix.cluster.impl;

import java.util.Map;

import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Cluster heartbeat message.
 */
final class ClusterHeartbeat {
  private final MemberId memberId;
  private final Member.Type type;
  private final String zone;
  private final String rack;
  private final String host;
  private final Map<String, String> metadata;

  ClusterHeartbeat(MemberId memberId, Member.Type type, String zone, String rack, String host, Map<String, String> metadata) {
    this.memberId = memberId;
    this.type = type;
    this.zone = zone;
    this.rack = rack;
    this.host = host;
    this.metadata = metadata;
  }

  /**
   * Returns the identifier of the member that sent the heartbeat.
   *
   * @return the identifier of the member that sent the heartbeat
   */
  public MemberId memberId() {
    return memberId;
  }

  /**
   * Returns the type of the member that sent the heartbeat.
   *
   * @return the member type
   */
  public Member.Type memberType() {
    return type;
  }

  /**
   * Returns the zone.
   *
   * @return the zone
   */
  public String zone() {
    return zone;
  }

  /**
   * Returns the rack.
   *
   * @return the rack
   */
  public String rack() {
    return rack;
  }

  /**
   * Returns the host.
   *
   * @return the host
   */
  public String host() {
    return host;
  }

  /**
   * Returns the member metadata.
   *
   * @return the member metadata
   */
  public Map<String, String> metadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("memberId", memberId)
        .add("type", type)
        .toString();
  }
}
