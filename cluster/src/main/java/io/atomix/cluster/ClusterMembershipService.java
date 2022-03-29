// SPDX-FileCopyrightText: 2014-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.utils.event.ListenerService;
import io.atomix.utils.net.Address;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service for obtaining information about the individual members within
 * the cluster.
 */
public interface ClusterMembershipService extends ListenerService<ClusterMembershipEvent, ClusterMembershipEventListener> {

  /**
   * Returns the local member.
   *
   * @return local member
   */
  Member getLocalMember();

  /**
   * Returns the set of current cluster members.
   *
   * @return set of cluster members
   */
  Set<Member> getMembers();

  /**
   * Returns the set of active reachable members.
   *
   * @return the set of active reachable members
   */
  default Set<Member> getReachableMembers() {
    return getMembers().stream()
        .filter(member -> member.isReachable())
        .collect(Collectors.toSet());
  }

  /**
   * Returns the specified member node.
   * <p>
   * This is a convenience method that wraps the given {@link String} in a {@link MemberId}. To avoid unnecessary
   * object allocation, repeated invocations of this method should instead use {@link #getMember(MemberId)}.
   *
   * @param memberId the member identifier
   * @return the member or {@code null} if no node with the given identifier exists
   */
  default Member getMember(String memberId) {
    return getMember(MemberId.from(memberId));
  }

  /**
   * Returns the specified member.
   *
   * @param memberId the member identifier
   * @return the member or {@code null} if no node with the given identifier exists
   */
  Member getMember(MemberId memberId);

  /**
   * Returns a member by address.
   *
   * @param address the member address
   * @return the member or {@code null} if no member with the given address could be found
   */
  default Member getMember(Address address) {
    return getMembers().stream()
        .filter(member -> member.address().equals(address))
        .findFirst()
        .orElse(null);
  }

}
