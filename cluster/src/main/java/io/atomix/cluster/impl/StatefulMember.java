// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.impl;

import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.utils.Version;
import io.atomix.utils.net.Address;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default cluster node.
 */
public class StatefulMember extends Member {
  private final Version version;
  private final AtomicLong timestamp = new AtomicLong();
  private volatile boolean active;
  private volatile boolean reachable;

  public StatefulMember(MemberId id, Address address) {
    super(id, address);
    this.version = null;
    timestamp.set(0);
  }

  public StatefulMember(
      MemberId id,
      Address address,
      String zone,
      String rack,
      String host,
      Properties properties,
      Version version) {
    super(id, address, zone, rack, host, properties);
    this.version = version;
    timestamp.set(1);
  }

  @Override
  public Version version() {
    return version;
  }

  /**
   * Returns the member logical timestamp.
   *
   * @return the member logical timestamp
   */
  public long getTimestamp() {
    return timestamp.get();
  }

  /**
   * Sets the member's logical timestamp.
   *
   * @param timestamp the member's logical timestamp
   */
  void setTimestamp(long timestamp) {
    this.timestamp.accumulateAndGet(timestamp, Math::max);
  }

  /**
   * Increments the member's timestamp.
   */
  void incrementTimestamp() {
    timestamp.incrementAndGet();
  }

  /**
   * Sets whether this member is an active member of the cluster.
   *
   * @param active whether this member is an active member of the cluster
   */
  void setActive(boolean active) {
    this.active = active;
  }

  /**
   * Sets whether this member is reachable.
   *
   * @param reachable whether this member is reachable
   */
  void setReachable(boolean reachable) {
    this.reachable = reachable;
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public boolean isReachable() {
    return reachable;
  }
}
