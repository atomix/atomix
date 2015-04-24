/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.EventListener;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Copycat cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Cluster {

  /**
   * Returns the local cluster member.<p>
   *
   * When messages are sent to the local member, Copycat will send the messages asynchronously on an event loop.
   *
   * @return The local cluster member.
   */
  LocalMember member();

  /**
   * Returns a member by member identifier.
   *
   * @param id The unique member identifier.
   * @return The member or {@code null} if the member does not exist.
   */
  Member member(int id);

  /**
   * Returns an immutable set of all cluster members.
   *
   * @return An immutable set of all members in the cluster.
   */
  Collection<Member> members();

  /**
   * Broadcasts a message to the cluster.<p>
   *
   * Message broadcasting to the Copycat cluster should not be considered reliable. Copycat sends broadcast messages to
   * all currently known members of the cluster in a fire-and-forget manner. Broadcast messages do not support replies.
   * To send messages with replies to all members of the cluster, iterate over the collection of
   * {@link net.kuujo.copycat.cluster.Cluster#members()} in the cluster.
   *
   * @param topic The topic to which to broadcast the message. Members with broadcast listeners registered for this
   *              topic will receive the broadcast message.
   * @param message The message to broadcast. This will be serialized using the serializer configured in the resource
   *                configuration.
   * @param <T> The message type.
   * @return The cluster.
   */
  <T> Cluster broadcast(String topic, T message);

  /**
   * Adds a membership listener to the cluster.<p>
   *
   * Membership listeners are triggered when {@link Member.Type#PASSIVE} members join or leave the cluster. Copycat uses
   * a gossip based failure detection algorithm to detect failures, using vector clocks to version cluster
   * configurations. In order to prevent false positives due to network partitions, Copycat's failure detection
   * algorithm will attempt to contact a member from up to three different nodes before considering that node failed.
   * If the membership listener is called with a {@link net.kuujo.copycat.cluster.MembershipChangeEvent.Type#LEAVE} event, that indicates that Copycat
   * has attempted to contact the missing member multiple times.<p>
   *
   * {@link Member.Type#ACTIVE} members never join or leave the cluster since they are explicitly configured, active,
   * voting members of the cluster. However, this may change at some point in the future to allow failure detection for
   * active members as well.
   *
   * @param listener The membership event listener to add.
   * @return The cluster.
   */
  Cluster addMembershipListener(EventListener<MembershipChangeEvent> listener);

  /**
   * Removes a membership listener from the cluster.
   *
   * @param listener The membership event listener to remove.
   * @return The cluster.
   */
  Cluster removeMembershipListener(EventListener<MembershipChangeEvent> listener);

  /**
   * Cluster builder.
   */
  public static interface Builder<BUILDER extends Builder<BUILDER, LOCAL, REMOTE>, LOCAL extends ManagedLocalMember, REMOTE extends ManagedRemoteMember> extends net.kuujo.copycat.Builder<ManagedCluster> {

    /**
     * Sets the local cluster member.
     *
     * @param member The local cluster member.
     * @return The cluster builder.
     */
    BUILDER withLocalMember(LOCAL member);

    /**
     * Sets the set of remote cluster members.
     *
     * @param members The set of remote cluster members.
     * @return The cluster builder.
     */
    default BUILDER withRemoteMembers(REMOTE... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return withRemoteMembers(Arrays.asList(members));
    }

    /**
     * Sets the set of remote cluster members.
     *
     * @param members The set of remote cluster members.
     * @return The cluster builder.
     */
    BUILDER withRemoteMembers(Collection<REMOTE> members);

    /**
     * Adds a remote member to the cluster.
     *
     * @param member The remote member to add.
     * @return The cluster builder.
     */
    default BUILDER addRemoteMember(REMOTE member) {
      return addRemoteMembers(Collections.singleton(member));
    }

    /**
     * Adds a set of remote members to the cluster.
     *
     * @param members The set of remote members to add.
     * @return The cluster builder.
     */
    default BUILDER addRemoteMembers(REMOTE... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return addRemoteMembers(Arrays.asList(members));
    }

    /**
     * Adds a set of remote members to the cluster.
     *
     * @param members The set of remote members to add.
     * @return The cluster builder.
     */
    BUILDER addRemoteMembers(Collection<REMOTE> members);

    /**
     * Sets the set of cluster members.
     *
     * @param members The set of cluster members.
     * @return The cluster builder.
     */
    default BUILDER withMembers(Member... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return withMembers(Arrays.asList(members));
    }

    /**
     * Sets the set of cluster members.
     *
     * @param members The set of cluster members.
     * @return The cluster builder.
     */
    BUILDER withMembers(Collection<Member> members);

    /**
     * Adds a member to the cluster.
     *
     * @param member The member to add.
     * @return The cluster builder.
     */
    default BUILDER addMember(Member member) {
      if (member == null)
        throw new NullPointerException("member cannot be null");
      return addMembers(Collections.singleton(member));
    }

    /**
     * Adds a set of members to the cluster.
     *
     * @param members The set of members to add.
     * @return The cluster builder.
     */
    default BUILDER addMembers(Member... members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      return addMembers(Arrays.asList(members));
    }

    /**
     * Adds a set of members to the cluster.
     *
     * @param members The set of members to add.
     * @return The cluster builder.
     */
    @SuppressWarnings("unchecked")
    default BUILDER addMembers(Collection<Member> members) {
      if (members == null)
        throw new NullPointerException("members cannot be null");
      members.forEach(m -> {
        if (m instanceof LocalMember) {
          withLocalMember((LOCAL) m);
        } else if (m instanceof RemoteMember) {
          addRemoteMember((REMOTE) m);
        }
      });
      return (BUILDER) this;
    }
  }

}
