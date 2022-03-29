// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip;

import com.google.common.annotations.Beta;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Configurable peer selectors.
 */
@Beta
public enum PeerSelectors implements PeerSelector {
  RANDOM {
    @Override
    public Collection<MemberId> select(Object entry, ClusterMembershipService membership) {
      List<MemberId> sortedMembers = membership.getMembers()
          .stream()
          .map(Member::id)
          .sorted()
          .collect(Collectors.toList());
      return Collections.singletonList(sortedMembers.get((int) Math.floor(Math.random() * sortedMembers.size())));
    }
  }
}
