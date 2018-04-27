/*
 * Copyright 2014-present Open Networking Foundation
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

import io.atomix.utils.event.ListenerService;

import java.util.Set;

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

}
