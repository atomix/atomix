// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.election.impl;

import io.atomix.core.election.Leadership;
import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

import java.util.Map;

/**
 * Leader elector service.
 */
public interface LeaderElectorService {

  /**
   * Attempts to become leader for a topic.
   *
   * @param topic leadership topic
   * @param id    instance identifier of the candidate
   * @return current Leadership state of the topic
   */
  @Command
  Leadership<byte[]> run(String topic, byte[] id);

  /**
   * Withdraws from leadership race for a topic.
   *
   * @param topic leadership topic
   * @param id    instance identifier of the candidate to withdraw
   */
  @Command
  void withdraw(String topic, byte[] id);

  /**
   * Attempts to promote a node to leadership displacing the current leader.
   *
   * @param topic leadership topic
   * @param id    instance identifier of the node to anoint
   * @return {@code true} if leadership transfer was successfully executed; {@code false} if it failed.
   * This operation can return {@code false} if the node to be made new leader is not registered to
   * run for election for the topic.
   */
  @Command
  boolean anoint(String topic, byte[] id);

  /**
   * Attempts to promote a node to top of candidate list.
   *
   * @param topic leadership topic
   * @param id    instance identifier of the node to promote
   * @return {@code true} if node is now the top candidate. This operation can fail (i.e. return
   * {@code false}) if the node is not registered to run for election for the topic.
   */
  @Command
  boolean promote(String topic, byte[] id);

  /**
   * Attempts to evict a node from all leadership elections it is registered for.
   * <p>
   * If the node the current leader for a topic, this call will force the next candidate (if one exists)
   * to be promoted to leadership.
   *
   * @param id instance identifier
   */
  @Command
  void evict(byte[] id);

  /**
   * Returns the {@link Leadership} for the specified topic.
   *
   * @param topic leadership topic
   * @return current Leadership state of the topic
   */
  @Query
  Leadership<byte[]> getLeadership(String topic);

  /**
   * Returns the current {@link Leadership}s for all topics.
   *
   * @return topic name to Leadership mapping
   */
  @Query
  Map<String, Leadership<byte[]>> getLeaderships();

  /**
   * Registers a listener to be notified of Leadership changes for all topics.
   */
  @Command
  void listen();

  /**
   * Unregisters a previously registered change notification listener.
   */
  @Command
  void unlisten();

}
