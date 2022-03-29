// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.partition;

import io.atomix.cluster.Member;
import io.atomix.primitive.partition.impl.NodeMemberGroup;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Member group strategy.
 * <p>
 * Member group strategies are default implementations of {@link MemberGroupProvider} for built-in node attributes.
 */
public enum MemberGroupStrategy implements MemberGroupProvider {

  /**
   * Zone aware member group strategy.
   * <p>
   * This strategy will create a member group for each unique zone in the cluster.
   */
  ZONE_AWARE {
    @Override
    public Collection<MemberGroup> getMemberGroups(Collection<Member> members) {
      return groupNodes(members, node -> node.zone() != null ? node.zone() : node.id().id());
    }
  },

  /**
   * Rack aware member group strategy.
   * <p>
   * This strategy will create a member group for each unique rack in the cluster.
   */
  RACK_AWARE {
    @Override
    public Collection<MemberGroup> getMemberGroups(Collection<Member> members) {
      return groupNodes(members, node -> node.rack() != null ? node.rack() : node.id().id());
    }
  },

  /**
   * Host aware member group strategy.
   * <p>
   * This strategy will create a member group for each unique host in the cluster.
   */
  HOST_AWARE {
    @Override
    public Collection<MemberGroup> getMemberGroups(Collection<Member> members) {
      return groupNodes(members, node -> node.host() != null ? node.host() : node.id().id());
    }
  },

  /**
   * Node aware member group strategy (the default).
   * <p>
   * This strategy will create a member group for each node in the cluster, effectively behaving the same as if
   * no member groups were defined.
   */
  NODE_AWARE {
    @Override
    public Collection<MemberGroup> getMemberGroups(Collection<Member> members) {
      return groupNodes(members, node -> node.id().id());
    }
  };

  /**
   * Groups nodes by the given key function.
   *
   * @param members       the nodes to group
   * @param keyFunction the key function to apply to nodes to extract a key
   * @return a collection of node member groups
   */
  protected Collection<MemberGroup> groupNodes(Collection<Member> members, Function<Member, String> keyFunction) {
    Map<String, Set<Member>> groups = new HashMap<>();
    for (Member member : members) {
      groups.computeIfAbsent(keyFunction.apply(member), k -> new HashSet<>()).add(member);
    }

    return groups.entrySet().stream()
        .map(entry -> new NodeMemberGroup(MemberGroupId.from(entry.getKey()), entry.getValue()))
        .collect(Collectors.toList());
  }
}
