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
 * The cluster contains state information defining the current cluster configuration. Note that cluster configuration
 * state is potential stale at any given point in time.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Cluster {

  /**
   * Returns the current cluster leader.
   *
   * @return The current cluster leader.
   */
  Member leader();

  /**
   * Returns the cluster term.
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
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  LocalMember member();

  /**
   * Returns a member by URI.
   *
   * @param uri The unique member URI.
   * @return The member.
   */
  Member member(String uri);

  /**
   * Returns an immutable set of all cluster members.
   *
   * @return An immutable set of all cluster members.
   */
  Members members();

  /**
   * Broadcasts a message to the cluster.
   *
   * @param topic The topic to which to broadcast the message.
   * @param message The message to broadcast.
   * @param <T> The message type.
   * @return The cluster.
   */
  <T> Cluster broadcast(String topic, T message);

  /**
   * Adds a broadcast listener to the cluster.
   *
   * @param topic The topic to which to listen.
   * @param listener The broadcast listener to add.
   * @param <T> The broadcast message type.
   * @return The cluster.
   */
  <T> Cluster addBroadcastListener(String topic, EventListener<T> listener);

  /**
   * Removes a broadcast listener from the cluster.
   *
   * @param topic The topic to which to listen.
   * @param listener The broadcast listener to remove.
   * @param <T> The broadcast message type.
   * @return The cluster.
   */
  <T> Cluster removeBroadcastListener(String topic, EventListener<T> listener);

  /**
   * Adds a membership listener to the cluster.
   *
   * @param listener The membership event listener.
   * @return The cluster.
   */
  Cluster addMembershipListener(EventListener<MembershipEvent> listener);

  /**
   * Removes a membership listener from the cluster.
   *
   * @param listener The membership event listener.
   * @return The cluster.
   */
  Cluster removeMembershipListener(EventListener<MembershipEvent> listener);

  /**
   * Adds an election listener to the cluster.
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
