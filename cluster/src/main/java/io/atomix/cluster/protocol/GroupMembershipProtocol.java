// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.protocol;

import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.discovery.NodeDiscoveryService;
import io.atomix.utils.ConfiguredType;
import io.atomix.utils.config.Configured;
import io.atomix.utils.event.ListenerService;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Group membership protocol.
 */
public interface GroupMembershipProtocol
    extends ListenerService<GroupMembershipEvent, GroupMembershipEventListener>,
    Configured<GroupMembershipProtocolConfig> {

  /**
   * Group membership protocol type.
   */
  interface Type<C extends GroupMembershipProtocolConfig> extends ConfiguredType<C> {

    /**
     * Creates a new instance of the protocol.
     *
     * @param config the protocol configuration
     * @return the protocol instance
     */
    GroupMembershipProtocol newProtocol(C config);
  }

  /**
   * Returns the set of current cluster members.
   *
   * @return set of cluster members
   */
  Set<Member> getMembers();

  /**
   * Returns the specified member.
   *
   * @param memberId the member identifier
   * @return the member or {@code null} if no node with the given identifier exists
   */
  Member getMember(MemberId memberId);

  /**
   * Joins the cluster.
   *
   * @param bootstrap the bootstrap service
   * @param discovery the discovery service
   * @param localMember the local member info
   * @return a future to be completed once the join is complete
   */
  CompletableFuture<Void> join(BootstrapService bootstrap, NodeDiscoveryService discovery, Member localMember);

  /**
   * Leaves the cluster.
   *
   * @param localMember the local member info
   * @return a future to be completed once the leave is complete
   */
  CompletableFuture<Void> leave(Member localMember);

}
