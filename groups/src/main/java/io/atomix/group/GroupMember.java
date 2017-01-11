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
package io.atomix.group;

import io.atomix.catalyst.annotations.Experimental;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.group.messaging.MessageClient;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * A {@link DistributedGroup} member representing a member of the group controlled by a local
 * or remote process.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface GroupMember {

  /**
   * Status constants for indicating whether a member is currently connected to the group.
   */
  enum Status {
    /**
     * Indicates that the member is alive and communicating with the group.
     */
    ALIVE,

    /**
     * Indicates that the member's session was expired. This status only applies to persistent members
     * that have been disconnected from the cluster.
     */
    DEAD,
  }

  /**
   * Returns the member ID.
   * <p>
   * The member ID is guaranteed to be unique across the cluster. Depending on how the member was
   * constructed, it may be a user-provided identifier or an automatically generated {@link java.util.UUID}.
   *
   * @return The member ID.
   */
  String id();

  /**
   * Returns the member's last known status.
   * <p>
   * The member's status indicates whether the member is {@link Status#ALIVE} or {@link Status#DEAD} based
   * on whether the member's session is open. Note that the consistency of client-side events is
   * {@code SEQUENTIAL}, meaning all group instances will see status changes in the same order some time
   * after the event actually occurred (asynchronously). So, even if a member's status is {@link Status#DEAD}
   * from the perspective of one node, it may be {@link Status#ALIVE} from the perspective of another.
   * Users should elect leaders to coordinate change in response to member status changes.
   *
   * @return The member status.
   */
  Status status();

  /**
   * Registers a member status change listener.
   * <p>
   * The status change listener will be called when a member's session expires.
   * <p>
   * The member's status indicates whether the member is {@link Status#ALIVE} or {@link Status#DEAD} based
   * on whether the member's session is open. Note that the consistency of client-side events is
   * {@code SEQUENTIAL}, meaning all group instances will see status changes in the same order some time
   * after the event actually occurred (asynchronously). So, even if a member's status is {@link Status#DEAD}
   * from the perspective of one node, it may be {@link Status#ALIVE} from the perspective of another.
   * Users should elect leaders to coordinate change in response to member status changes.
   *
   * @param callback The callback to be called when the member's status changes.
   * @return The status listener context.
   */
  Listener<Status> onStatusChange(Consumer<Status> callback);

  /**
   * Returns the direct message client for this member.
   * <p>
   * The message client can be used to send direct messages to this member. To send a direct message, create
   * a producer via the returned {@link MessageClient}.
   * <pre>
   *   {@code
   *   GroupMember member = group.member("foo");
   *   MessageProducer<String> producer = member.messaging().producer("bar");
   *   producer.send("baz");
   *   }
   * </pre>
   *
   * @return The direct message client for this member.
   */
  @Experimental
  MessageClient messaging();

  /**
   * Returns the metadata associated with this member.
   * <p>
   * Metadata is provided when the member {@link DistributedGroup#join(Object) join}s the group. If metadata was
   * provided by the member when it joined the group, it is guaranteed to be visible to all nodes once the member
   * has been added to the group.
   *
   * @param <T> The metadata type.
   * @return The member metadata.
   */
  <T> Optional<T> metadata();

}
