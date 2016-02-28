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
package io.atomix.coordination;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link DistributedGroup} member representing a member of the group controlled by a local
 * or remote process.
 * <p>
 * A {@code GroupMember} represents a reference to a single instance of a resource which has
 * {@link DistributedGroup#join() joined} a membership group. Each member is guaranteed to
 * have a unique {@link #id()} throughout the lifetime of the distributed resource. Group members
 * can {@link #schedule(Duration, Runnable) schedule} or {@link #execute(Runnable) execute} callbacks
 * remotely on member nodes.
 * <p>
 * Group members support direct messaging via the {@link #send(String, Object)} method. Messages
 * sent between group members must be associated with a {@link String} topic. The topic is used
 * to identify the type of message being sent.
 * <pre>
 *   {@code
 *   group.member("foo").send("bar", "Hello world!");
 *   }
 * </pre>
 * Messages sent between group members are persisted and replicated in the cluster and are guaranteed
 * to be received by the member as long as its session remains open.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface GroupMember {

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
   * Returns a boolean value indicating whether this member is the current leader.
   * <p>
   * Whether this member is the current leader is dependent on the last known configuration of the membership
   * group. If the local group instance is partitioned from the cluster, it may believe a member to be the
   * leader when it has in fact been replaced.
   *
   * @return Indicates whether this member is the current leader.
   */
  boolean isLeader();

  /**
   * Gets the value of a property of the member.
   * <p>
   * Properties are identified by a {@link String} name. Properties may only be set by the local member but
   * can be accessed by any instance of the {@link DistributedGroup}.
   *
   * @param property The property to get.
   * @param <T> The property type.
   * @return A completable future to be completed with the value of the property.
   */
  <T> CompletableFuture<T> get(String property);

  /**
   * Returns a collection of partitions assigned to the member.
   * <p>
   * The returned collection of partitions can change over time as partitions are reassigned when members
   * {@link DistributedGroup#join() join} or {@link LocalGroupMember#leave() leave} the group. Users should
   * call this method each time the member's partitions are being accessed. When a member is added to or
   * removed from the group, partitions will not be added until <em>after</em> the group change event has
   * occurred, so listening to member join/leave events to determine when partitions change is unreliable.
   *
   * @return A collection of partitions assigned to the member.
   */
  Collection<Partition> partitions();

  /**
   * Sends a message to the member.
   * <p>
   * Group messaging guarantees sequential consistency such that members are guaranteed to receive messages
   * in the order in which they were sent. Messages will be sent according to the parent {@link DistributedGroup}'s
   * {@link io.atomix.resource.WriteConsistency consistency} level. If the consistency level is
   * {@link io.atomix.resource.WriteConsistency#ATOMIC} (the default), the returned {@link CompletableFuture} will
   * be completed once the member has received the message or has left the group. Note that the completion of
   * the returned future does not necessarily guarantee that the message was received and processed, only that
   * it was <em>either</em> received and processed <em>or</em> the member left the group or disconnected.
   * <pre>
   *   {@code
   *   group.member("foo").send("bar", "Hello world!");
   *   }
   * </pre>
   *
   * @param topic The message topic.
   * @param message The message to send.
   * @return A completable future to be completed once the message has been received by the member.
   */
  CompletableFuture<Void> send(String topic, Object message);

  /**
   * Schedules a callback to run at the given instant.
   *
   * @param instant The instant at which to run the callback.
   * @param callback The callback to run.
   * @return A completable future to be completed once the callback has been scheduled.
   */
  CompletableFuture<Void> schedule(Instant instant, Runnable callback);

  /**
   * Schedules a callback to run after the given delay on the member.
   *
   * @param delay The delay after which to run the callback.
   * @param callback The callback to run.
   * @return A completable future to be completed once the callback has been scheduled.
   */
  CompletableFuture<Void> schedule(Duration delay, Runnable callback);

  /**
   * Executes a callback on the group member.
   *
   * @param callback The callback to execute.
   * @return A completable future to be completed once the callback has completed.
   */
  CompletableFuture<Void> execute(Runnable callback);

}
