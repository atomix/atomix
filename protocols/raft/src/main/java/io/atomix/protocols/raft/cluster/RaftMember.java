/*
 * Copyright 2016-present Open Networking Laboratory
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
 * limitations under the License
 */
package io.atomix.protocols.raft.cluster;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Represents a member of a Raft cluster.
 * <p>
 * This interface provides metadata and operations related to a specific member of a Raft cluster.
 * Each server in a {@link RaftCluster} has a view of the cluster state and can reference and operate on
 * specific members of the cluster via this API.
 * <p>
 * Each member in the cluster has an associated {@link RaftMember.Type} and {@link RaftMember.Status}. The
 * {@link RaftMember.Type} is indicative of the manner in which the member interacts with other members of
 * the cluster. The {@link RaftMember.Status} is indicative of the leader's ability to communicate with the
 * member. Additionally, each member is identified by an {@link #getMemberId() address} and unique {@link #getMemberId() ID}
 * which is generated from the {@link MemberId} hash code. The member's {@link MemberId} represents the
 * address through which the server communicates with other servers and not through which clients
 * communicate with the member (which may be a different {@link MemberId}).
 * <p>
 * Users can listen for {@link RaftMember.Type} and {@link RaftMember.Status} changes via the
 * {@link #addTypeChangeListener(Consumer)} and {@link #addStatusChangeListener(Consumer)} methods respectively.
 * Member types can be modified by virtually any member of the cluster via the {@link #promote()} and {@link #demote()}
 * methods. This allows servers to modify the way dead nodes interact with the cluster and modify the
 * Raft quorum size without requiring the member being modified to be available. The member status is
 * controlled only by the cluster {@link RaftCluster#getLeader() leader}. When the leader fails to contact a
 * member for a few rounds of heartbeats, the leader will commit a configuration change marking that
 * member as {@link RaftMember.Status#UNAVAILABLE}. Once the member can be reached again, the leader will
 * update its status back to {@link RaftMember.Status#AVAILABLE}.
 */
public interface RaftMember {

  /**
   * Indicates how the member participates in voting and replication.
   * <p>
   * The member type defines how a member interacts with the other members of the cluster and, more
   * importantly, how the cluster {@link RaftCluster#getLeader() leader} interacts with the member server.
   * Members can be {@link #promote() promoted} and {@link #demote() demoted} to alter member states.
   * See the specific member types for descriptions of their implications on the cluster.
   */
  enum Type {

    /**
     * Represents an inactive member.
     * <p>
     * The {@code INACTIVE} member type represents a member which does not participate in any communication
     * and is not an active member of the cluster. This is typically the state of a member prior to joining
     * or after leaving a cluster.
     */
    INACTIVE,

    /**
     * Represents a member which does not participate in replication.
     * <p>
     * The {@code RESERVE} member type is representative of a member that does not participate in any
     * replication of state but only maintains contact with the cluster leader and is an active member
     * of the {@link RaftCluster}. Typically, reserve members act as standby nodes which can be
     * {@link #promote() promoted} to a {@link #PASSIVE} or {@link #ACTIVE} role when needed.
     */
    RESERVE,

    /**
     * Represents a member which participates in asynchronous replication but does not vote in elections
     * or otherwise participate in the Raft consensus algorithm.
     * <p>
     * The {@code PASSIVE} member type is representative of a member that receives state changes from
     * follower nodes asynchronously. As state changes are committed via the {@link #ACTIVE} Raft nodes,
     * committed state changes are asynchronously replicated by followers to passive members. This allows
     * passive members to maintain nearly up-to-date state with minimal impact on the performance of the
     * Raft algorithm itself, and allows passive members to be quickly promoted to {@link #ACTIVE} voting
     * members if necessary.
     */
    PASSIVE,

    /**
     * Represents a full voting member of the Raft cluster which participates fully in leader election
     * and replication algorithms.
     * <p>
     * The {@code ACTIVE} member type represents a full voting member of the Raft cluster. Active members
     * participate in the Raft leader election and replication algorithms and can themselves be elected
     * leaders.
     */
    ACTIVE,

  }

  /**
   * Indicates the availability of a member from the perspective of the cluster {@link RaftCluster#getLeader() leader}.
   * <p>
   * Member statuses are manged by the cluster {@link RaftCluster#getLeader() leader}. For each {@link RaftMember} of a
   * {@link RaftCluster}, the leader periodically sends a heartbeat to the member to determine its availability.
   * In the event that the leader cannot contact a member for more than a few heartbeats, the leader will
   * set the member's availability status to {@link #UNAVAILABLE}. Once the leader reestablishes communication
   * with the member, it will reset its status back to {@link #AVAILABLE}.
   */
  enum Status {

    /**
     * Indicates that a member is reachable by the leader.
     * <p>
     * Availability is determined by the leader's ability to successfully send heartbeats to the member. If the
     * last heartbeat attempt to the member was successful, its status will be available. For members whose status
     * is {@link #UNAVAILABLE}, once the leader is able to heartbeat the member its status will be reset to available.
     */
    AVAILABLE,

    /**
     * Indicates that a member is unreachable by the leader.
     * <p>
     * Availability is determined by the leader's ability to successfully send heartbeats to the member. If the
     * leader repeatedly fails to heartbeat a member, the leader will eventually commit a configuration change setting
     * the member's status to unavailable. Once the leader is able to contact the member again, its status will be
     * reset to {@link #AVAILABLE}.
     */
    UNAVAILABLE,

  }

  /**
   * Returns the member node ID.
   *
   * @return The member node ID.
   */
  MemberId getMemberId();

  /**
   * Returns the member hash.
   *
   * @return The member hash.
   */
  int getHash();

  /**
   * Returns the member type.
   * <p>
   * The member type is indicative of the member's level of participation in the Raft consensus algorithm and
   * asynchronous replication within the cluster. Member types may change throughout the lifetime of the cluster.
   * Types can be changed by {@link #promote(Type) promoting} or {@link #demote(Type) demoting} the member. Member
   * types for a given member are guaranteed to change in the same order on all nodes, but the type of a member
   * may be different from the perspective of different nodes at any given time.
   *
   * @return The member type.
   */
  Type getType();

  /**
   * Adds a listener to be called when the member's type changes.
   * <p>
   * The type change callback will be called when the local server receives notification of the change in type
   * to this member. Type changes may occur at different times from the perspective of different servers but are
   * guaranteed to occur in the same order on all servers.
   *
   * @param listener The listener to be called when the member's type changes.
   */
  void addTypeChangeListener(Consumer<Type> listener);

  /**
   * Removes a type change listener from the member.
   *
   * @param listener The listener to remove from the member.
   */
  void removeTypeChangeListener(Consumer<Type> listener);

  /**
   * Returns the member status.
   * <p>
   * The status is indicative of the leader's ability to communicate with this member. If this member is a local
   * member, the member's status will be {@link Status#AVAILABLE} while the server is alive and will not change
   * regardless of the leader's ability to communicate with the local member. Similarly, if the local server is
   * partitioned from the leader then changes in statuses seen on other nodes may not be visible to this node.
   * Status changes are guaranteed to occur in the same order on all nodes but without a real-time constraint.
   *
   * @return The member status.
   */
  Status getStatus();

  /**
   * Returns the time at which the member was updated.
   * <p>
   * The member update time is not guaranteed to be consistent across servers or consistent across server
   * restarts. The update time is guaranteed to be monotonically increasing.
   *
   * @return The time at which the member was updated.
   */
  Instant getLastUpdated();

  /**
   * Adds a listener to be called when the member's status changes.
   * <p>
   * The status change callback will be called when the local server receives notification of the change in status
   * to this member. Status changes may occur at different times from the perspective of different servers but are
   * guaranteed to occur in the same order on all servers.
   *
   * @param listener The listener to be called when the member's status changes.
   */
  void addStatusChangeListener(Consumer<Status> listener);

  /**
   * Removes a status change listener.
   *
   * @param listener The status change listener to remove.
   */
  void removeStatusChangeListener(Consumer<Status> listener);

  /**
   * Promotes the member to the next highest type.
   * <p>
   * If the member is promoted to {@link Type#ACTIVE} the Raft quorum size will increase.
   *
   * @return A completable future to be completed once the member has been promoted.
   */
  CompletableFuture<Void> promote();

  /**
   * Promotes the member to the given type.
   * <p>
   * If the member is promoted to {@link Type#ACTIVE} the Raft quorum size will increase.
   *
   * @param type The type to which to promote the member.
   * @return A completable future to be completed once the member has been promoted.
   */
  CompletableFuture<Void> promote(RaftMember.Type type);

  /**
   * Demotes the member to the next lowest type.
   * <p>
   * If the member is an {@link Type#ACTIVE} member then demoting it will impact the Raft quorum size.
   *
   * @return A completable future to be completed once the member has been demoted.
   */
  CompletableFuture<Void> demote();

  /**
   * Demotes the member to the given type.
   * <p>
   * If the member is an {@link Type#ACTIVE} member then demoting it will impact the Raft quorum size.
   *
   * @param type The type to which to demote the member.
   * @return A completable future to be completed once the member has been demoted.
   */
  CompletableFuture<Void> demote(RaftMember.Type type);

  /**
   * Removes the member from the configuration.
   * <p>
   * If the member is a part of the current Raft quorum (is an {@link Type#ACTIVE} member) then the
   * quorum will be impacted by removing the member.
   *
   * @return A completable future to be completed once the member has been removed from the configuration.
   */
  CompletableFuture<Void> remove();

}
