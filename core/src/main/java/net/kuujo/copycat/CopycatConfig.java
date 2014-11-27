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
package net.kuujo.copycat;

import net.kuujo.copycat.spi.QuorumStrategy;

/**
 * Copycat configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CopycatConfig extends Copyable<CopycatConfig> {

  /**
   * Sets whether a quorum replication is required for command operations.<p>
   *
   * If command quorums are disabled, the leader will forgo replicating individual commands to followers
   * during writes. When a command is received by the leader, it will immediately log the command and return
   * the result, eventually replicating the command asynchronously. <em>Disabling command quorums can cause
   * inconsistency of state</em> between replicas. While disabling quorums will allow a leader to remain
   * available even while a majority of the cluster is unavailable, once the cluster is healed data may
   * be overwritten. Thus it is strongly recommended that command quorums not be disabled.
   *
   * @param require Indicates whether a quorum replication should be required for commands.
   */
  void setRequireCommandQuorum(boolean require);

  /**
   * Returns a boolean indicating whether a quorum replication is required for command
   * operations.<p>
   *
   * If command quorums are disabled, the leader will forgo replicating individual commands to followers
   * during writes. When a command is received by the leader, it will immediately log the command and return
   * the result, eventually replicating the command asynchronously. <em>Disabling command quorums can cause
   * inconsistency of state</em> between replicas. While disabling quorums will allow a leader to remain
   * available even while a majority of the cluster is unavailable, once the cluster is healed data may
   * be overwritten. Thus it is strongly recommended that command quorums not be disabled.
   *
   * @return Indicates whether a quorum replication is required for command operations.
   */
  boolean isRequireCommandQuorum();

  /**
   * Sets whether a quorum replication is required for command operations, returning the
   * configuration for method chaining.<p>
   *
   * If command quorums are disabled, the leader will forgo replicating individual commands to followers
   * during writes. When a command is received by the leader, it will immediately log the command and return
   * the result, eventually replicating the command asynchronously. <em>Disabling command quorums can cause
   * inconsistency of state</em> between replicas. While disabling quorums will allow a leader to remain
   * available even while a majority of the cluster is unavailable, once the cluster is healed data may
   * be overwritten. Thus it is strongly recommended that command quorums not be disabled.
   *
   * @param require Indicates whether a quorum replication should be required for commands.
   * @return The Copycat configuration.
   */
  CopycatConfig withRequireCommandQuorum(boolean require);

  /**
   * Sets the fixed required command quorum size.<p>
   *
   * The quorum size is the number of replicas to which a command must be replicated before it will be
   * considered committed. Rather than calculating a quorum size based on the cluster configuration, users
   * can set a fixed quorum size for commands.
   *
   * @param quorumSize The required command quorum size.
   * @throws IllegalArgumentException if {@code quorumSize} is not >= -1
   */
  void setCommandQuorumSize(int quorumSize);

  /**
   * Returns the required command quorum size.<p>
   *
   * The quorum size is the number of replicas to which a command must be replicated before it will be
   * considered committed. Rather than calculating a quorum size based on the cluster configuration, users
   * can set a fixed quorum size for commands.
   *
   * @return The required command quorum size. Defaults to <code>-1</code>
   */
  int getCommandQuorumSize();

  /**
   * Sets the required command quorum size, returning the configuration for method chaining.<p>
   *
   * The quorum size is the number of replicas to which a command must be replicated before it will be
   * considered committed. Rather than calculating a quorum size based on the cluster configuration, users
   * can set a fixed quorum size for commands.
   *
   * @param quorumSize The required command quorum size.
   * @return The copycat configuration.
   * @throws IllegalArgumentException if {@code quorumSize} is not > -1
   */
  CopycatConfig withCommandQuorumSize(int quorumSize);

  /**
   * Sets the cluster command quorum strategy.<p>
   *
   * The quorum strategy is used by the replica to calculate the number of nodes to which a command must be
   * replicated before it can be considered committed. By default, the calculated quorum size is
   * <code>Math.min(clusterSize / 2) + 1</code> - commands must be replicated to a majority of the cluster in
   * order to be considered committed. <em>It is strongly recommended that quorum sizes remain at least
   * greater than half the cluster size.</em> Reducing the quorum size to less than a majority of the cluster
   * membership may result in data loss.
   *
   * @param strategy The cluster command quorum calculation strategy.
   * @throws NullPointerException if {@code strategy} is null
   */
  void setCommandQuorumStrategy(QuorumStrategy strategy);

  /**
   * Returns the cluster command quorum strategy.<p>
   *
   * The quorum strategy is used by the replica to calculate the number of nodes to which a command must be
   * replicated before it can be considered committed. By default, the calculated quorum size is
   * <code>Math.min(clusterSize / 2) + 1</code> - commands must be replicated to a majority of the cluster in
   * order to be considered committed. <em>It is strongly recommended that quorum sizes remain at least
   * greater than half the cluster size.</em> Reducing the quorum size to less than a majority of the cluster
   * membership may result in data loss.
   *
   * @return The cluster command quorum calculation strategy.
   */
  QuorumStrategy getCommandQuorumStrategy();

  /**
   * Sets the cluster command quorum strategy, returning the configuration for method chaining.<p>
   *
   * The quorum strategy is used by the replica to calculate the number of nodes to which a command must be
   * replicated before it can be considered committed. By default, the calculated quorum size is
   * <code>Math.min(clusterSize / 2) + 1</code> - commands must be replicated to a majority of the cluster in
   * order to be considered committed. <em>It is strongly recommended that quorum sizes remain at least
   * greater than half the cluster size.</em> Reducing the quorum size to less than a majority of the cluster
   * membership may result in data loss.
   *
   * @param strategy The cluster command quorum calculation strategy.
   * @return The copycat configuration.
   * @throws NullPointerException if {@code strategy} is null
   */
  CopycatConfig withCommandQuorumStrategy(QuorumStrategy strategy);

  /**
   * Sets whether to use consistent command execution.<p>
   *
   * Consistent command execution configures a high level consistency behavior. If consistent command execution
   * is enabled, command quorums will be enabled and the command quorum size will always be a majority of the cluster.
   * This ensures the strongest guarantees of consistency for operations that contribute to the state machine state.
   *
   * @param consistent Whether to use consistent command execution.
   */
  void setConsistentCommandExecution(boolean consistent);

  /**
   * Returns whether consistent command execution is enabled.<p>
   *
   * Consistent command execution configures a high level consistency behavior. If consistent command execution
   * is enabled, command quorums will be enabled and the command quorum size will always be a majority of the cluster.
   * This ensures the strongest guarantees of consistency for operations that contribute to the state machine state.
   *
   * @return Indicates whether consistent command execution is enabled.
   */
  boolean isConsistentCommandExecution();

  /**
   * Sets whether to use consistent command execution, returning the configuration for method chaining.
   *
   * @param consistent Whether to use consistent command execution.
   * @return The Copycat configuration.
   */
  CopycatConfig withConsistentCommandExecution(boolean consistent);

  /**
   * Sets whether a quorum synchronization is required for query operations.<p>
   *
   * This property indicates whether the leader must synchronize with followers during read-only operations.
   * If query quorums are enabled, when the leader receives a read-only request it will ping a quorum of the
   * cluster prior to responding with the read result. Quorums can be configured via static configuration or
   * the strategy pattern.
   *
   * @param require Indicates whether a quorum synchronization should be required for query
   *          operations.
   */
  void setRequireQueryQuorum(boolean require);

  /**
   * Returns a boolean indicating whether a quorum synchronization is required for query
   * operations.<p>
   *
   * This property indicates whether the leader must synchronize with followers during read-only operations.
   * If query quorums are enabled, when the leader receives a read-only request it will ping a quorum of the
   * cluster prior to responding with the read result. Quorums can be configured via static configuration or
   * the strategy pattern.
   *
   * @return Indicates whether a quorum synchronization is required for query operations.
   */
  boolean isRequireQueryQuorum();

  /**
   * Sets whether a quorum synchronization is required for query operations, returning
   * the configuration for method chaining.<p>
   *
   * This property indicates whether the leader must synchronize with followers during read-only operations.
   * If query quorums are enabled, when the leader receives a read-only request it will ping a quorum of the
   * cluster prior to responding with the read result. Quorums can be configured via static configuration or
   * the strategy pattern.
   *
   * @param require Indicates whether a quorum synchronization should be required for query
   *          operations.
   * @return The replica configuration.
   */
  CopycatConfig withRequireQueryQuorum(boolean require);

  /**
   * Sets the required query quorum size.<p>
   *
   * The quorum size is the number of replicas that must be pinged during a read-only query operation. Reducing the
   * quorum size for query operations can help improve read performance, but there is a slightly greater potential
   * for inconsistent reads.
   *
   * @param quorumSize The required query quorum size.
   * @throws IllegalArgumentException if {@code quorumSize} is not > -1
   */
  void setQueryQuorumSize(int quorumSize);

  /**
   * Returns the required query quorum size.<p>
   *
   * The quorum size is the number of replicas that must be pinged during a read-only query operation. Reducing the
   * quorum size for query operations can help improve read performance, but there is a slightly greater potential
   * for inconsistent reads.
   *
   * @return The required query quorum size. Defaults to <code>null</code>
   */
  int getQueryQuorumSize();

  /**
   * Sets the required query quorum size, returning the configuration for method chaining.<p>
   *
   * The quorum size is the number of replicas that must be pinged during a read-only query operation. Reducing the
   * quorum size for query operations can help improve read performance, but there is a slightly greater potential
   * for inconsistent reads.
   *
   * @param quorumSize The required query quorum size.
   * @return The copycat configuration.
   * @throws IllegalArgumentException if {@code quorumSize} is not > -1
   */
  CopycatConfig withQueryQuorumSize(int quorumSize);

  /**
   * Sets the cluster query quorum strategy.<p>
   *
   * The quorum size is the number of replicas that must be pinged during a read-only query operation. Using the
   * strategy pattern, users can provide dynamic quorum sizes based on the mutable cluster configuration. By default,
   * the query quorum size is a majority of the cluster. Reducing the quorum size for query operations can help
   * improve read performance, but there is a slightly greater potential for inconsistent reads.
   *
   * @param strategy The cluster query quorum calculation strategy.
   * @throws NullPointerException if {@code strategy} is null
   */
  void setQueryQuorumStrategy(QuorumStrategy strategy);

  /**
   * Returns the cluster query quorum strategy.<p>
   *
   * The quorum size is the number of replicas that must be pinged during a read-only query operation. Using the
   * strategy pattern, users can provide dynamic quorum sizes based on the mutable cluster configuration. By default,
   * the query quorum size is a majority of the cluster. Reducing the quorum size for query operations can help
   * improve read performance, but there is a slightly greater potential for inconsistent reads.
   *
   * @return The cluster query quorum calculation strategy.
   */
  QuorumStrategy getQueryQuorumStrategy();

  /**
   * Sets the cluster query quorum strategy, returning the configuration for method chaining.<p>
   *
   * The quorum size is the number of replicas that must be pinged during a read-only query operation. Using the
   * strategy pattern, users can provide dynamic quorum sizes based on the mutable cluster configuration. By default,
   * the query quorum size is a majority of the cluster. Reducing the quorum size for query operations can help
   * improve read performance, but there is a slightly greater potential for inconsistent reads.
   *
   * @param strategy The cluster query quorum calculation strategy.
   * @return The Copycat configuration.
   * @throws NullPointerException if {@code strategy} is null
   */
  CopycatConfig withQueryQuorumStrategy(QuorumStrategy strategy);

  /**
   * Sets whether to use consistent query execution.<p>
   *
   * Normally, all command <em>and</em> query operations must go through the cluster leader. However, disabling
   * consistent query execution allows clients to perform query operations on followers. This can dramatically
   * increase performance in read-heavy systems at the risk of exposing stale data to the user.
   *
   * @param consistent Whether to use consistent query execution.
   */
  void setConsistentQueryExecution(boolean consistent);

  /**
   * Returns whether consistent query execution is enabled.<p>
   *
   * Normally, all command <em>and</em> query operations must go through the cluster leader. However, disabling
   * consistent query execution allows clients to perform query operations on followers. This can dramatically
   * increase performance in read-heavy systems at the risk of exposing stale data to the user.
   *
   * @return Indicates whether consistent query execution is enabled.
   */
  boolean isConsistentQueryExecution();

  /**
   * Sets whether to use consistent query execution, returning the configuration for method chaining.<p>
   *
   * Normally, all command <em>and</em> query operations must go through the cluster leader. However, disabling
   * consistent query execution allows clients to perform query operations on followers. This can dramatically
   * increase performance in read-heavy systems at the risk of exposing stale data to the user.
   *
   * @param consistent Whether to use consistent query execution.
   * @return The Copycat configuration.
   */
  CopycatConfig withConsistentQueryExecution(boolean consistent);

}
