// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip.map;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import io.atomix.cluster.MemberId;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Anti-entropy advertisement message for eventually consistent map.
 */
public class AntiEntropyAdvertisement {

  private final MemberId sender;
  private final Map<String, MapValue.Digest> digest;

  /**
   * Creates a new anti entropy advertisement message.
   *
   * @param sender the sender's node ID
   * @param digest for map entries
   */
  public AntiEntropyAdvertisement(MemberId sender,
      Map<String, MapValue.Digest> digest) {
    this.sender = checkNotNull(sender);
    this.digest = ImmutableMap.copyOf(checkNotNull(digest));
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
   * Returns the digest for map entries.
   *
   * @return mapping from key to associated digest
   */
  public Map<String, MapValue.Digest> digest() {
    return digest;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("sender", sender)
        .add("totalEntries", digest.size())
        .toString();
  }
}
