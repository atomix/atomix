// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip;

import com.google.common.annotations.Beta;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;

import java.util.Collection;

/**
 * Peer selector.
 */
@Beta
@FunctionalInterface
public interface PeerSelector<E> {

  /**
   * Selects the peers to update for the given entry.
   *
   * @param entry the entry for which to select peers
   * @param membership the cluster membership service
   * @return a collection of peers to update
   */
  Collection<MemberId> select(E entry, ClusterMembershipService membership);

}
