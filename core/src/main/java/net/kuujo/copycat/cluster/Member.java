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

import net.kuujo.copycat.Task;
import net.kuujo.copycat.raft.RaftMember;

import java.util.concurrent.CompletableFuture;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Member {

  /**
   * Cluster member type.<p>
   *
   * The member type indicates how cluster members behave in terms of joining and leaving the cluster and how the
   * members participate in log replication. {@link Type#ACTIVE} members are full voting members of the cluster that
   * participate in Copycat's consensus protocol. {@link Type#PASSIVE} members may join and leave the cluster at will
   * without impacting the availability of a resource and receive only committed log entries via a gossip protocol.
   */
  public static enum Type {

    /**
     * Indicates that the member is a passive, non-voting member of the cluster.
     */
    PASSIVE,

    /**
     * Indicates that the member is attempting to join the cluster.
     */
    PROMOTABLE,

    /**
     * Indicates that the member is an active voting member of the cluster.
     */
    ACTIVE;

    /**
     * Loops up the Member type for the given Raft member type.
     *
     * @param type The Raft member type.
     * @return The member type.
     */
    public static Type lookup(RaftMember.Type type) {
      switch (type) {
        case PASSIVE:
          return PASSIVE;
        case PROMOTABLE:
          return PROMOTABLE;
        case ACTIVE:
          return ACTIVE;
      }
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Cluster member status.<p>
   *
   * The member status indicates how a given member is perceived by the local node. Members can be in one of three states
   * at any given time, {@link net.kuujo.copycat.cluster.Member.Status#ALIVE}, {@link net.kuujo.copycat.cluster.Member.Status#SUSPICIOUS}, and {@link net.kuujo.copycat.cluster.Member.Status#DEAD}. Member states are changed
   * according to the local node's ability to communicate with a given member. All members begin with an
   * {@link net.kuujo.copycat.cluster.Member.Status#ALIVE} status upon joining the cluster. If the member appears to be unreachable, its status will be
   * changed to {@link net.kuujo.copycat.cluster.Member.Status#SUSPICIOUS}, indicating that it may have left the cluster or died. Once enough other nodes
   * in the cluster agree that the suspicious member appears to be dead, the status will be changed to {@link net.kuujo.copycat.cluster.Member.Status#DEAD}
   * and the member will ultimately be removed from the cluster configuration.
   */
  public static enum Status {

    /**
     * Indicates that the member is considered to be dead.
     */
    DEAD,

    /**
     * Indicates that the member is suspicious and is unreachable by at least one other member.
     */
    SUSPICIOUS,

    /**
     * Indicates that the member is alive and reachable.
     */
    ALIVE;

    /**
     * Looks up the member status for the given Raft member status.
     *
     * @param status The Raft member status.
     * @return The member status.
     */
    public static Status lookup(RaftMember.Status status) {
      switch (status) {
        case DEAD:
          return DEAD;
        case SUSPICIOUS:
          return SUSPICIOUS;
        case ALIVE:
          return ALIVE;
      }
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Returns the unique member ID.
   *
   * @return The unique member ID.
   */
  String id();

  /**
   * Returns the member address.
   *
   * @return The member address.
   */
  String address();

  /**
   * Returns the member type.
   *
   * @return The member type.
   */
  Type type();

  /**
   * Returns the member status.
   *
   * @return The member status.
   */
  Status status();

  /**
   * Sends a message to the member.<p>
   *
   * Messages are sent using a topic based messaging system over the configured cluster protocol. If no handler is
   * registered for the given topic on the given member, the returned {@link java.util.concurrent.CompletableFuture}
   * will be failed. If the member successfully receives the message and responds, the returned
   * {@link java.util.concurrent.CompletableFuture} will be completed with the member's response.
   *
   * @param topic The topic on which to send the message.
   * @param message The message to send. Messages will be serialized using the configured resource serializer.
   * @param <T> The message type.
   * @param <U> The response type.
   * @return A completable future to be completed with the message response.
   */
  <T, U> CompletableFuture<U> send(String topic, T message);

  /**
   * Executes a task on the member.
   *
   * @param task The task to execute.
   * @return A completable future to be completed once the task has been executed.
   */
  CompletableFuture<Void> execute(Task<Void> task);

  /**
   * Submits a task to the member, returning a future to be completed with the remote task result.
   *
   * @param task The task to submit to the member.
   * @param <T> The task return type.
   * @return A completable future to be completed with the task result.
   */
  <T> CompletableFuture<T> submit(Task<T> task);

}
