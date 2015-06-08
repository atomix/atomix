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

/**
 * Copycat cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Cluster extends Members {

  /**
   * Returns the local cluster member.<p>
   *
   * When messages are sent to the local member, Copycat will send the messages asynchronously on an event loop.
   *
   * @return The local cluster member.
   */
  LocalMember member();

  /**
   * Broadcasts a message to the cluster.<p>
   *
   * Message broadcasting to the Copycat cluster should not be considered reliable. Copycat sends broadcast messages to
   * all currently known members of the cluster in a fire-and-forget manner. Broadcast messages do not support replies.
   * To send messages with replies to all members of the cluster, iterate over the collection of
   * {@link net.kuujo.copycat.cluster.Cluster#members()} in the cluster.
   *
   * @param message The message to broadcast. This will be serialized using the serializer configured in the resource
   *                configuration.
   * @param <T> The message type.
   * @return The cluster.
   */
  <T> Cluster broadcast(T message);

  /**
   * Broadcasts a message to the cluster.<p>
   *
   * Message broadcasting to the Copycat cluster should not be considered reliable. Copycat sends broadcast messages to
   * all currently known members of the cluster in a fire-and-forget manner. Broadcast messages do not support replies.
   * To send messages with replies to all members of the cluster, iterate over the collection of
   * {@link net.kuujo.copycat.cluster.Cluster#members()} in the cluster.
   *
   * @param type The message type to broadcast.
   * @param message The message to broadcast. This will be serialized using the serializer configured in the resource
   *                configuration.
   * @param <T> The message type.
   * @return The cluster.
   */
  <T> Cluster broadcast(Class<? super T> type, T message);

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

  @Override
  Cluster addListener(MembershipListener listener);

  @Override
  Cluster removeListener(MembershipListener listener);

  /**
   * Cluster builder.
   */
  public static interface Builder<T extends Builder<T, U, V>, U extends ManagedCluster, V extends ManagedMember> extends Members.Builder<T, U, V> {

    /**
     * Sets the local member ID.
     *
     * @param id The local member ID.
     * @return The cluster builder.
     */
    T withMemberId(int id);

  }

}
