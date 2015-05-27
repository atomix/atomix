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

import java.util.concurrent.CompletableFuture;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Member {

  /**
   * Member type.
   */
  static enum Type {

    /**
     * Client member.
     */
    CLIENT,

    /**
     * Passive member.
     */
    PASSIVE,

    /**
     * Active member.
     */
    ACTIVE

  }

  /**
   * Member status.
   */
  static enum Status {

    /**
     * Indicates that the member is alive.
     */
    ALIVE,

    /**
     * Indicates that the member is dead.
     */
    DEAD

  }

  /**
   * Returns the unique member ID.
   *
   * @return The unique member ID.
   */
  int id();

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
   * Returns the member info.
   *
   * @return The member info.
   */
  MemberInfo info();

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

  /**
   * Member builder.
   */
  public static abstract interface Builder<T extends Builder<T, U>, U extends Member> extends net.kuujo.copycat.Builder<U> {

    /**
     * Sets the member identifier.
     *
     * @param id The member identifier.
     * @return The member builder.
     */
    T withId(int id);

  }

}
