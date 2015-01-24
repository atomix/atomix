/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.election.ElectionEvent;

/**
 * Resource cluster.<p>
 *
 * Each resource within a Copycat cluster contains a view of the cluster from that resource's perspective. Membership
 * in a resource cluster may differ from the global cluster configuration if the resource's replica set differs from
 * that of the master cluster or if the resource has not been opened on all nodes in the cluster. Members contained
 * within the resource cluster participate in log replication for the resource. {@code ACTIVE} members actively
 * participate in elections and consensus for the resource. {@code PASSIVE} members receive committed log entries via
 * a simple gossip protocol.
 *
 * The local member can be accessed via {@link net.kuujo.copycat.cluster.Cluster#member()}.<p>
 *
 * <pre>
 *   {@code
 *     LocalMember member = cluster.member();
 *   }
 * </pre>
 *
 * To get the member type, access a member and use the {@link net.kuujo.copycat.cluster.Member#type()} method.<p>
 *
 * <pre>
 *   {@code
 *     if (member.type() == Member.Type.ACTIVE) {
 *       // This member participates in Raft
 *     } else {
 *       // This member receives logs via gossip
 *     }
 *   }
 * </pre>
 *
 * While {@code ACTIVE} members never change, {@code PASSIVE} members may be added to or removed from the cluster
 * arbitrarily. Users can listen for passive members joining or leaving the cluster by adding a membership listener.<p>
 *
 * <pre>
 *   {@code
 *     cluster.addMembershipListener(event -> {
 *       if (event.type() == MembershipEvent.Type.JOIN) {
 *         LOGGER.info("{} joined the cluster", event.member().uri());
 *       } else {
 *         LOGGER.info("{} left the cluster", event.member().uri());
 *       }
 *     });
 *   }
 * </pre>
 *
 * Each cluster contains a reference to the respective resource's {@link net.kuujo.copycat.election.Election} instance.
 * Users can use the {@code Election} to identify the current cluster leader and receive event notifications when the
 * leadership for the resource's cluster changes.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Cluster {

  /**
   * Returns a reference to the current cluster leader.
   *
   * @return The current cluster leader. If there is no current leader for the resource's cluster then {@code null}
   * will be returned.
   */
  Member leader();

  /**
   * Returns the current cluster term.<p>
   *
   * The term is a monotonically increasing number that Copycat uses internally to coordinate leader elections and
   * resolve log conflicts. Copycat guarantees that only one leader will be elected for any given term.
   *
   * @return The cluster term.
   */
  long term();

  /**
   * Returns the cluster election.
   *
   * @return The cluster election.
   */
  Election election();

  /**
   * Returns the local cluster member.<p>
   *
   * When messages are sent to the local member, Copycat will send the messages asynchronously on an event loop.
   *
   * @return The local cluster member.
   */
  LocalMember member();

  /**
   * Returns a member by URI.
   *
   * @param uri The unique member URI.
   * @return The member or {@code null} if the member does not exist.
   * @throws java.lang.NullPointerException If the given {@code uri} is {@code null}
   */
  Member member(String uri);

  /**
   * Returns an immutable set of all cluster members.
   *
   * @return An immutable set of all members in the cluster.
   */
  Members members();

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
   * Adds a broadcast listener to the cluster.<p>
   *
   * Broadcast listeners are a special type of handler which receives messages broadcast to a topic by other nodes.
   *
   * @param topic The topic to which to listen. Messages broadcast to this topic will be received by the given event
   *              listener, including messages broadcast from the local node. Listeners cannot reply to broadcast
   *              messages.
   * @param listener The broadcast listener to add.
   * @param <T> The broadcast message type.
   * @return The cluster.
   */
  <T> Cluster addBroadcastListener(String topic, EventListener<T> listener);

  /**
   * Removes a broadcast listener from the cluster.
   *
   * @param topic The topic for which to remove the listener.
   * @param listener The broadcast listener to remove.
   * @param <T> The broadcast message type.
   * @return The cluster.
   */
  <T> Cluster removeBroadcastListener(String topic, EventListener<T> listener);

  /**
   * Adds a membership listener to the cluster.<p>
   *
   * Membership listeners are triggered when {@link Member.Type#PASSIVE} members join or leave the cluster. Copycat uses
   * a gossip based failure detection algorithm to detect failures, using vector clocks to version cluster
   * configurations. In order to prevent false positives due to network partitions, Copycat's failure detection
   * algorithm will attempt to contact a member from up to three different nodes before considering that node failed.
   * If the membership listener is called with a {@link MembershipEvent.Type#LEAVE} event, that indicates that Copycat
   * has attempted to contact the missing member multiple times.<p>
   *
   * {@link Member.Type#ACTIVE} members never join or leave the cluster since they are explicitly configured, active,
   * voting members of the cluster. However, this may change at some point in the future to allow failure detection for
   * active members as well.
   *
   * @param listener The membership event listener to add.
   * @return The cluster.
   */
  Cluster addMembershipListener(EventListener<MembershipEvent> listener);

  /**
   * Removes a membership listener from the cluster.
   *
   * @param listener The membership event listener to remove.
   * @return The cluster.
   */
  Cluster removeMembershipListener(EventListener<MembershipEvent> listener);

  /**
   * Adds an election listener to the cluster.<p>
   *
   * The election listener will receive an {@link net.kuujo.copycat.election.ElectionEvent} the first time any member
   * becomes the cluster leader for a given term. Copycat guarantees that for each election term only one member will
   * ever be elected. If a new member is elected, the term will have increased.
   *
   * @param listener The election event listener.
   * @return The cluster.
   */
  Cluster addElectionListener(EventListener<ElectionEvent> listener);

  /**
   * Removes an election listener from the cluster.
   *
   * @param listener The election event listener.
   * @return The cluster.
   */
  Cluster removeElectionListener(EventListener<ElectionEvent> listener);

}
