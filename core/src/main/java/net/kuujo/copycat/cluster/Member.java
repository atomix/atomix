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

import net.kuujo.copycat.Task;

import java.util.concurrent.CompletableFuture;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Member {

  /**
   * Member state.
   */
  public static enum State {
    LISTENER,
    PROMOTABLE,
    MEMBER,
    LEADER
  }

  /**
   * Returns the member URI.
   *
   * @return The member URI.
   */
  String uri();

  /**
   * Returns the member state.
   *
   * @return The member state.
   */
  State state();

  /**
   * Sends a message to the member.
   *
   * @param topic The message topic.
   * @param message The message to send.
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
   * Submits a task to the member.
   *
   * @param task The task to commit on the member.
   * @param <T> The task return type.
   * @return A completable future to be completed with the task result.
   */
  <T> CompletableFuture<T> submit(Task<T> task);

}
