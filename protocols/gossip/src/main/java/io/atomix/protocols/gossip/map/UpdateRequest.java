// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip.map;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import io.atomix.cluster.MemberId;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Describes a request for update events in an EventuallyConsistentMap.
 */
final class UpdateRequest<K> {

  private final MemberId sender;
  private final Set<K> keys;

  /**
   * Creates a new update request.
   *
   * @param sender the sender's node ID
   * @param keys   keys requested
   */
  UpdateRequest(MemberId sender, Set<K> keys) {
    this.sender = checkNotNull(sender);
    this.keys = ImmutableSet.copyOf(keys);
  }

  /**
   * Returns the sender's node ID.
   *
   * @return the sender's node ID
   */
  public MemberId sender() {
    return sender;
  }

  /**
   * Returns the keys.
   *
   * @return the keys
   */
  public Set<K> keys() {
    return keys;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("sender", sender)
        .add("keys", keys())
        .toString();
  }

  @SuppressWarnings("unused")
  private UpdateRequest() {
    this.sender = null;
    this.keys = null;
  }
}
