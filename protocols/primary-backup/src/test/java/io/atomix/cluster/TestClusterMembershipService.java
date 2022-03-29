// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster;

import io.atomix.utils.net.Address;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test cluster service.
 */
public class TestClusterMembershipService implements ClusterMembershipService {
  private final MemberId localNode;
  private final Collection<MemberId> nodes;

  public TestClusterMembershipService(MemberId localNode, Collection<MemberId> nodes) {
    this.localNode = localNode;
    this.nodes = nodes;
  }

  @Override
  public Member getLocalMember() {
    return Member.builder(localNode)
        .withAddress(Address.from("localhost", localNode.hashCode()))
        .build();
  }

  @Override
  public Set<Member> getMembers() {
    return nodes.stream()
        .map(node -> Member.builder(node)
            .withAddress(Address.from("localhost", node.hashCode()))
            .build())
        .collect(Collectors.toSet());
  }

  @Override
  public Member getMember(MemberId memberId) {
    return null;
  }

  @Override
  public void addListener(ClusterMembershipEventListener listener) {

  }

  @Override
  public void removeListener(ClusterMembershipEventListener listener) {

  }
}
