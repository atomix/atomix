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
