/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.election;

import io.atomix.primitive.SyncPrimitive;

/**
 * {@code LeaderElector} provides the same functionality as {@link AsyncLeaderElection} with
 * the only difference that all its methods block until the corresponding operation completes.
 */
public interface LeaderElection<T> extends SyncPrimitive {

  /**
   * Attempts to become leader for a topic.
   *
   * @param identifier candidate identifier
   * @return current Leadership state of the topic
   */
  Leadership<T> run(T identifier);

  /**
   * Withdraws from leadership race for a topic.
   *
   * @param identifier identifier of the node to withdraw
   */
  void withdraw(T identifier);

  /**
   * Attempts to promote a node to leadership displacing the current leader.
   *
   * @param identifier identifier of the new leader
   * @return {@code true} if leadership transfer was successfully executed; {@code false} if it failed.
   * This operation can return {@code false} if the node to be made new leader is not registered to
   * run for election for the topic.
   */
  boolean anoint(T identifier);

  /**
   * Attempts to promote a node to top of candidate list.
   *
   * @param identifier identifier of the new top candidate
   * @return {@code true} if node is now the top candidate. This operation can fail (i.e. return
   * {@code false}) if the node is not registered to run for election for the topic.
   */
  boolean promote(T identifier);

  /**
   * Attempts to evict a node from all leadership elections it is registered for.
   * <p>
   * If the node the current leader for a topic, this call will force the next candidate (if one exists)
   * to be promoted to leadership.
   *
   * @param identifier identifier
   */
  void evict(T identifier);

  /**
   * Returns the {@link Leadership} for the specified topic.
   *
   * @return current Leadership state of the topic
   */
  Leadership<T> getLeadership();

  /**
   * Registers a listener to be notified of Leadership changes for all topics.
   *
   * @param listener listener to add
   */
  void addListener(LeadershipEventListener<T> listener);

  /**
   * Unregisters a previously registered change notification listener.
   * <p>
   * If the specified listener was not previously registered, this operation will be a noop.
   *
   * @param listener listener to remove
   */
  void removeListener(LeadershipEventListener<T> listener);

  @Override
  AsyncLeaderElection<T> async();
}
