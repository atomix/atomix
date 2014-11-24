/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.spi.CorrelationStrategy;
import net.kuujo.copycat.spi.QuorumStrategy;
import net.kuujo.copycat.spi.TimerStrategy;

/**
 * Copycat replica configuration.<p>
 *
 * This is the primary configuration for the algorithm underlying Copycat replicas.
 * By default, this configuration is set up to ensure strong consistency of the state machine.
 * However, various options allow users to make consistency concessions in return for performance
 * improvements. The individual method documentation will describe the benefits and drawbacks
 * of each configuration option.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatConfig {

  /**
   * Property name for the timeout within which a leader election occurs.
   */
  public static final String ELECTION_TIMEOUT = "copycat.election.timeout";

  /**
   * Property name for the interval at which the replica sends heartbeats to other nodes in the cluster.
   */
  public static final String HEARTBEAT_INTERVAL = "copycat.heartbeat.interval";

  /**
   * Property name for indicating whether quorums are required for command operations.
   */
  public static final String COMMAND_QUORUM = "copycat.command.quorum";

  /**
   * Property name for configuring the command quorum size.
   */
  public static final String COMMAND_QUORUM_SIZE = "copycat.command.quorum.size";

  /**
   * Property name for specifying a command quorum strategy implementation.
   */
  public static final String COMMAND_QUORUM_STRATEGY = "copycat.command.quorum.strategy";

  /**
   * Property name for indicating whether consistent execution of commands is enabled.
   */
  public static final String COMMAND_CONSISTENT_EXECUTION = "copycat.command.consistent_execution";

  /**
   * Property name for indicating whether quorums are required for query operations.
   */
  public static final String QUERY_QUORUM = "copycat.query.quorum";

  /**
   * Property name for configuring the query quorum size.
   */
  public static final String QUERY_QUORUM_SIZE = "copycat.query.quorum.size";

  /**
   * Property name for specifying a query quorum strategy implementation.
   */
  public static final String QUERY_QUORUM_STRATEGY = "copycat.query.quorum.strategy";

  /**
   * Property name for indicating whether consistent execution of queries is enabled.
   */
  public static final String QUERY_CONSISTENT_EXECUTION = "copycat.query.consistent_execution";

  /**
   * Property name for specifying the maximum log file size.
   */
  public static final String MAX_LOG_SIZE = "copycat.log.max_size";

  /**
   * Property name for specifying a correlation strategy implementation.
   */
  public static final String CORRELATION_STRATEGY = "copycat.correlation.strategy";

  /**
   * Property name for specifying a timer strategy implementation.
   */
  public static final String TIMER_STRATEGY = "copycat.timer.strategy";

  private static class ClassLoaderProcessor<T> implements Function<String, T> {
    @Override
    @SuppressWarnings("unchecked")
    public T apply(String value) {
      try {
        return (T) Thread.currentThread().getContextClassLoader().loadClass(value).newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private static class PropertyWrapper<T> {
    private final Predicate<String> validator;
    private final Function<String, T> processor;
    private final Consumer setter;

    private PropertyWrapper(Predicate<String> validator, Function<String, T> processor, Consumer<T> setter) {
      this.validator = validator;
      this.processor = processor;
      this.setter = setter;
    }
  }

  @SuppressWarnings("serial")
  private final Map<String, PropertyWrapper<?>> processors = new HashMap<String, PropertyWrapper<?>>() {{
    put(ELECTION_TIMEOUT, new PropertyWrapper<Long>(null, Long::valueOf, CopycatConfig.this::setElectionTimeout));
    put(HEARTBEAT_INTERVAL, new PropertyWrapper<Long>(null, Long::valueOf, CopycatConfig.this::setHeartbeatInterval));
    put(COMMAND_QUORUM, new PropertyWrapper<Boolean>(null, Boolean::parseBoolean, CopycatConfig.this::setRequireCommandQuorum));
    put(COMMAND_QUORUM_SIZE, new PropertyWrapper<Integer>(null, Integer::valueOf, CopycatConfig.this::setCommandQuorumSize));
    put(COMMAND_QUORUM_STRATEGY, new PropertyWrapper<QuorumStrategy>(null, new ClassLoaderProcessor<>(), CopycatConfig.this::setCommandQuorumStrategy));
    put(COMMAND_CONSISTENT_EXECUTION, new PropertyWrapper<>(null, Boolean::parseBoolean, CopycatConfig.this::setConsistentCommandExecution));
    put(QUERY_QUORUM, new PropertyWrapper<Boolean>(null, Boolean::parseBoolean, CopycatConfig.this::setRequireQueryQuorum));
    put(QUERY_QUORUM_SIZE, new PropertyWrapper<Integer>(null, Integer::valueOf, CopycatConfig.this::setQueryQuorumSize));
    put(QUERY_QUORUM_STRATEGY, new PropertyWrapper<QuorumStrategy>(null, new ClassLoaderProcessor<>(), CopycatConfig.this::setQueryQuorumStrategy));
    put(QUERY_CONSISTENT_EXECUTION, new PropertyWrapper<>(null, Boolean::parseBoolean, CopycatConfig.this::setConsistentQueryExecution));
    put(MAX_LOG_SIZE, new PropertyWrapper<Long>(null, Long::valueOf, CopycatConfig.this::setElectionTimeout));
    put(CORRELATION_STRATEGY, new PropertyWrapper<CorrelationStrategy<?>>(null, new ClassLoaderProcessor<>(), CopycatConfig.this::setCorrelationStrategy));
    put(TIMER_STRATEGY, new PropertyWrapper<TimerStrategy>(null, new ClassLoaderProcessor<>(), CopycatConfig.this::setTimerStrategy));
  }};

  private static final ThreadFactory THREAD_FACTORY = new NamedThreadFactory("config-timer-%s");
  private static final QuorumStrategy DEFAULT_QUORUM_STRATEGY = (cluster) -> (int) Math.floor(cluster.size() / 2) + 1;
  private static final CorrelationStrategy<?> DEFAULT_CORRELATION_STRATEGY = () -> UUID.randomUUID().toString();

  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);
  @SuppressWarnings("unchecked")
  private final TimerStrategy DEFAULT_TIMER_STRATEGY = (task, delay, unit) -> (ScheduledFuture<Void>) scheduler.schedule(task, delay, unit);
  private long electionTimeout = 2000;
  private long heartbeatInterval = 500;
  private boolean requireCommandQuorum = true;
  private int commandQuorumSize = -1;
  private QuorumStrategy commandQuorumStrategy = DEFAULT_QUORUM_STRATEGY;
  private boolean consistentCommandExecution = true;
  private boolean requireQueryQuorum = true;
  private int queryQuorumSize = -1;
  private QuorumStrategy queryQuorumStrategy = DEFAULT_QUORUM_STRATEGY;
  private boolean consistentQueryExecution = true;
  private int maxLogSize = 32 * 1024^2;
  private CorrelationStrategy<?> correlationStrategy = DEFAULT_CORRELATION_STRATEGY;
  private TimerStrategy timerStrategy = DEFAULT_TIMER_STRATEGY;

  public CopycatConfig() {
  }

  /**
   * Constructs a Copycat configuration from properties.
   *
   * @param properties Copycat configuration properties.
   */
  @SuppressWarnings("unchecked")
  public CopycatConfig(Properties properties) {
    for (Map.Entry<String, PropertyWrapper<?>> entry : processors.entrySet()) {
      String value = properties.getProperty(entry.getKey());
      if (value != null) {
        PropertyWrapper<?> property = entry.getValue();
        if (property.validator != null) {
          if (!property.validator.test(value)) {
            throw new CopycatException("Invalid property value for " + entry.getKey());
          }
        }
        property.setter.accept(property.processor.apply(value));
      }
    }
  }

  /**
   * Sets the replica election timeout.<p>
   *
   * This is the timeout within which a vote must occur. When a replica becomes a candidate in an election,
   * the election timer will be started. If the election timer expires before the election round has completed
   * (i.e. enough votes were received) then a new election round will begin.
   * 
   * @param timeout The election timeout in milliseconds.
   * @throws IllegalArgumentException if {@code timeout} is not > 0
   */
  public void setElectionTimeout(long timeout) {
    this.electionTimeout = Assert.arg(timeout, timeout > 0, "Election timeout must be positive");
  }

  /**
   * Returns the replica election timeout.<p>
   *
   * This is the timeout within which a vote must occur. When a replica becomes a candidate in an election,
   * the election timer will be started. If the election timer expires before the election round has completed
   * (i.e. enough votes were received) then a new election round will begin.
   * 
   * @return The election timeout.
   */
  public long getElectionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the replica election timeout, returning the configuration for method chaining.<p>
   *
   * This is the timeout within which a vote must occur. When a replica becomes a candidate in an election,
   * the election timer will be started. If the election timer expires before the election round has completed
   * (i.e. enough votes were received) then a new election round will begin.
   * 
   * @param timeout The election timeout in milliseconds.
   * @return The copycat configuration.
   * @throws IllegalArgumentException if {@code timeout} is not > 0
   */
  public CopycatConfig withElectionTimeout(long timeout) {
    this.electionTimeout = Assert.arg(timeout, timeout > 0, "Election timeout must be positive");
    return this;
  }

  /**
   * Sets the replica heartbeat interval.<p>
   *
   * This is the interval at which the replica sends heartbeat messages to other nodes in the cluster. Note that
   * this property only applies to leaders as all heartbeats only flow from leader to followers.
   * 
   * @param interval The interval at which the node should send heartbeat messages in milliseconds.
   * @throws IllegalArgumentException if {@code interval} is not > 0
   */
  public void setHeartbeatInterval(long interval) {
    this.heartbeatInterval = Assert.arg(interval, interval > 0, "Heart beat interval must be positive");
  }

  /**
   * Returns the replica heartbeat interval.<p>
   *
   * This is the interval at which the replica sends heartbeat messages to other nodes in the cluster. Note that
   * this property only applies to leaders as all heartbeats only flow from leader to followers.
   * 
   * @return The replica heartbeat interval.
   */
  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Sets the replica heartbeat interval, returning the configuration for method chaining.<p>
   *
   * This is the interval at which the replica sends heartbeat messages to other nodes in the cluster. Note that
   * this property only applies to leaders as all heartbeats only flow from leader to followers.
   * 
   * @param interval The interval at which the node should send heartbeat messages in milliseconds.
   * @return The replica configuration.
   * @throws IllegalArgumentException if {@code interval} is not > 0
   */
  public CopycatConfig withHeartbeatInterval(long interval) {
    this.heartbeatInterval = Assert.arg(interval, interval > 0, "Heart beat interval must be positive");
    return this;
  }

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
  public void setRequireCommandQuorum(boolean require) {
    this.requireCommandQuorum = require;
  }

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
  public boolean isRequireCommandQuorum() {
    return requireCommandQuorum;
  }

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
  public CopycatConfig withRequireCommandQuorum(boolean require) {
    this.requireCommandQuorum = require;
    return this;
  }

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
  public void setCommandQuorumSize(int quorumSize) {
    this.commandQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.commandQuorumStrategy = (config) -> commandQuorumSize;
  }

  /**
   * Returns the required command quorum size.<p>
   *
   * The quorum size is the number of replicas to which a command must be replicated before it will be
   * considered committed. Rather than calculating a quorum size based on the cluster configuration, users
   * can set a fixed quorum size for commands.
   *
   * @return The required command quorum size. Defaults to <code>-1</code>
   */
  public int getCommandQuorumSize() {
    return commandQuorumSize;
  }

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
  public CopycatConfig withCommandQuorumSize(int quorumSize) {
    this.commandQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.commandQuorumStrategy = (config) -> commandQuorumSize;
    return this;
  }

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
  public void setCommandQuorumStrategy(QuorumStrategy strategy) {
    this.commandQuorumStrategy = Assert.isNotNull(strategy, "strategy");
  }

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
  public QuorumStrategy getCommandQuorumStrategy() {
    return commandQuorumStrategy;
  }

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
  public CopycatConfig withCommandQuorumStrategy(QuorumStrategy strategy) {
    this.commandQuorumStrategy = Assert.isNotNull(strategy, "strategy");
    return this;
  }

  /**
   * Sets whether to use consistent command execution.<p>
   *
   * Consistent command execution configures a high level consistency behavior. If consistent command execution
   * is enabled, command quorums will be enabled and the command quorum size will always be a majority of the cluster.
   * This ensures the strongest guarantees of consistency for operations that contribute to the state machine state.
   *
   * @param consistent Whether to use consistent command execution.
   */
  public void setConsistentCommandExecution(boolean consistent) {
    this.consistentCommandExecution = consistent;
    if (consistent) {
      setRequireCommandQuorum(true);
      this.commandQuorumSize = -1;
      this.commandQuorumStrategy = DEFAULT_QUORUM_STRATEGY;
    } else {
      setRequireCommandQuorum(false);
    }
  }

  /**
   * Returns whether consistent command execution is enabled.<p>
   *
   * Consistent command execution configures a high level consistency behavior. If consistent command execution
   * is enabled, command quorums will be enabled and the command quorum size will always be a majority of the cluster.
   * This ensures the strongest guarantees of consistency for operations that contribute to the state machine state.
   *
   * @return Indicates whether consistent command execution is enabled.
   */
  public boolean isConsistentCommandExecution() {
    return consistentCommandExecution;
  }

  /**
   * Sets whether to use consistent command execution, returning the configuration for method chaining.
   *
   * @param consistent Whether to use consistent command execution.
   * @return The Copycat configuration.
   */
  public CopycatConfig withConsistentCommandExecution(boolean consistent) {
    setConsistentCommandExecution(consistent);
    return this;
  }

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
  public void setRequireQueryQuorum(boolean require) {
    this.requireQueryQuorum = require;
  }

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
  public boolean isRequireQueryQuorum() {
    return requireQueryQuorum;
  }

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
  public CopycatConfig withRequireQueryQuorum(boolean require) {
    this.requireQueryQuorum = require;
    return this;
  }

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
  public void setQueryQuorumSize(int quorumSize) {
    this.queryQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.queryQuorumStrategy = (config) -> queryQuorumSize;
  }

  /**
   * Returns the required query quorum size.<p>
   *
   * The quorum size is the number of replicas that must be pinged during a read-only query operation. Reducing the
   * quorum size for query operations can help improve read performance, but there is a slightly greater potential
   * for inconsistent reads.
   *
   * @return The required query quorum size. Defaults to <code>null</code>
   */
  public int getQueryQuorumSize() {
    return queryQuorumSize;
  }

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
  public CopycatConfig withQueryQuorumSize(int quorumSize) {
    this.queryQuorumSize = Assert.arg(quorumSize, quorumSize > -1, "Quorum size must be -1 or greater");
    this.queryQuorumStrategy = (config) -> queryQuorumSize;
    return this;
  }

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
  public void setQueryQuorumStrategy(QuorumStrategy strategy) {
    this.queryQuorumStrategy = Assert.isNotNull(strategy, "strategy");
  }

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
  public QuorumStrategy getQueryQuorumStrategy() {
    return queryQuorumStrategy;
  }

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
  public CopycatConfig withQueryQuorumStrategn(QuorumStrategy strategy) {
    this.queryQuorumStrategy = Assert.isNotNull(strategy, "strategy");
    return this;
  }

  /**
   * Sets whether to use consistent query execution.<p>
   *
   * Normally, all command <em>and</em> query operations must go through the cluster leader. However, disabling
   * consistent query execution allows clients to perform query operations on followers. This can dramatically
   * increase performance in read-heavy systems at the risk of exposing stale data to the user.
   *
   * @param consistent Whether to use consistent query execution.
   */
  public void setConsistentQueryExecution(boolean consistent) {
    this.consistentQueryExecution = consistent;
    if (consistent) {
      setRequireQueryQuorum(true);
      this.queryQuorumStrategy = DEFAULT_QUORUM_STRATEGY;
    } else {
      setRequireQueryQuorum(false);
    }
  }

  /**
   * Returns whether consistent query execution is enabled.<p>
   *
   * Normally, all command <em>and</em> query operations must go through the cluster leader. However, disabling
   * consistent query execution allows clients to perform query operations on followers. This can dramatically
   * increase performance in read-heavy systems at the risk of exposing stale data to the user.
   *
   * @return Indicates whether consistent query execution is enabled.
   */
  public boolean isConsistentQueryExecution() {
    return consistentQueryExecution;
  }

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
  public CopycatConfig withConsistentQueryExecution(boolean consistent) {
    setConsistentQueryExecution(consistent);
    return this;
  }

  /**
   * Sets the maximum log size.<p>
   *
   * This is the maximum size to which the log can grow (in bytes) before Copycat will compact the log. Once the log
   * has grown to the given size, the replica will take a snapshot of the state machine state by calling
   * {@link StateMachine#takeSnapshot()}, append the snapshot to the log, and remove all entries prior to the snapshot.
   * This allows Copycat's logs to expand infinitely.
   *
   * @param maxSize The maximum local log size.
   * @throws IllegalArgumentException if {@code maxSize} is not > 0
   */
  public void setMaxLogSize(int maxSize) {
    this.maxLogSize = Assert.arg(maxSize, maxSize > 0, "Max log size must be positive");
  }

  /**
   * Returns the maximum log size.<p>
   *
   * This is the maximum size to which the log can grow (in bytes) before Copycat will compact the log. Once the log
   * has grown to the given size, the replica will take a snapshot of the state machine state by calling
   * {@link StateMachine#takeSnapshot()}, append the snapshot to the log, and remove all entries prior to the snapshot.
   * This allows Copycat's logs to expand infinitely.
   *
   * @return The maximum local log size.
   */
  public int getMaxLogSize() {
    return maxLogSize;
  }

  /**
   * Sets the maximum log size, returning the configuration for method chaining.<p>
   *
   * This is the maximum size to which the log can grow (in bytes) before Copycat will compact the log. Once the log
   * has grown to the given size, the replica will take a snapshot of the state machine state by calling
   * {@link StateMachine#takeSnapshot()}, append the snapshot to the log, and remove all entries prior to the snapshot.
   * This allows Copycat's logs to expand infinitely.
   *
   * @param maxSize The maximum local log size.
   * @return The replica configuration.
   * @throws IllegalArgumentException if {@code maxSize} is not > 0
   */
  public CopycatConfig withMaxLogSize(int maxSize) {
    this.maxLogSize = Assert.arg(maxSize, maxSize > 0, "Max log size must be positive");
    return this;
  }

  /**
   * Sets the message correlation strategy.<p>
   *
   * This is a strategy for creating unique correlation identifiers for Copycat's internal
   * {@link net.kuujo.copycat.protocol.Request} and {@link net.kuujo.copycat.protocol.Response} instances. By
   * default, Copycat uses a {@link java.util.UUID} based correlation strategy, but simple counters are more
   * efficient and should work fine with most protocol implementations.
   *
   * @param strategy The message correlation strategy.
   * @throws NullPointerException if {@code strategy} is null
   */
  public void setCorrelationStrategy(CorrelationStrategy<?> strategy) {
    this.correlationStrategy = Assert.isNotNull(strategy, "strategy");
  }

  /**
   * Returns the message correlation strategy.<p>
   *
   * This is a strategy for creating unique correlation identifiers for Copycat's internal
   * {@link net.kuujo.copycat.protocol.Request} and {@link net.kuujo.copycat.protocol.Response} instances. By
   * default, Copycat uses a {@link java.util.UUID} based correlation strategy, but simple counters are more
   * efficient and should work fine with most protocol implementations.
   *
   * @return The message correlation strategy.
   */
  public CorrelationStrategy<?> getCorrelationStrategy() {
    return correlationStrategy;
  }

  /**
   * Sets the message correlation strategy, returning the configuration for method chaining.<p>
   *
   * This is a strategy for creating unique correlation identifiers for Copycat's internal
   * {@link net.kuujo.copycat.protocol.Request} and {@link net.kuujo.copycat.protocol.Response} instances. By
   * default Copycat uses a {@link java.util.UUID} based correlation strategy, but simple counters are more
   * efficient and should work fine with most protocol implementations.
   *
   * @param strategy The message correlation strategy.
   * @return The copycat configuration.
   * @throws NullPointerException if {@code strategy} is null
   */
  public CopycatConfig withCorrelationStrategy(CorrelationStrategy<?> strategy) {
    this.correlationStrategy = Assert.isNotNull(strategy, "strategy");
    return this;
  }

  /**
   * Sets the timer strategy.<p>
   *
   * This is the timer on which Copycat schedules elections, heartbeats, and other time-dependent operations. By
   * default Copycat uses a {@link java.util.concurrent.ScheduledExecutorService} to schedule operations. However,
   * asynchronous environments such as Netty have their own event loop based timers, and the timer strategy can be
   * used to run Copycat timers on the event loop.
   *
   * @param strategy The timer strategy.
   * @throws NullPointerException if {@code strategy} is null
   */
  public void setTimerStrategy(TimerStrategy strategy) {
    this.timerStrategy = Assert.isNotNull(strategy, "strategy");
  }

  /**
   * Returns the timer strategy.<p>
   *
   * This is the timer on which Copycat schedules elections, heartbeats, and other time-dependent operations. By
   * default Copycat uses a {@link java.util.concurrent.ScheduledExecutorService} to schedule operations. However,
   * asynchronous environments such as Netty have their own event loop based timers, and the timer strategy can be
   * used to run Copycat timers on the event loop.
   *
   * @return The timer strategy.
   */
  public TimerStrategy getTimerStrategy() {
    return timerStrategy;
  }

  /**
   * Sets the timer strategy, returning the configuration for method chaining.<p>
   *
   * This is the timer on which Copycat schedules elections, heartbeats, and other time-dependent operations. By
   * default Copycat uses a {@link java.util.concurrent.ScheduledExecutorService} to schedule operations. However,
   * asynchronous environments such as Netty have their own event loop based timers, and the timer strategy can be
   * used to run Copycat timers on the event loop.
   *
   * @param strategy The timer strategy.
   * @return The copycat configuration.
   * @throws NullPointerException if {@code strategy} is null
   */
  public CopycatConfig withTimerStrategy(TimerStrategy strategy) {
    this.timerStrategy = Assert.isNotNull(strategy, "strategy");
    return this;
  }

  @Override
  public String toString() {
    String value = "CopycatConfig";
    value += "[\n";
    value += String.format("electionTimeout=%d", electionTimeout);
    value += ",\n";
    value += String.format("heartbeatInterval=%d", heartbeatInterval);
    value += ",\n";
    value += String.format("requireQueryQuorum=%s", requireQueryQuorum);
    value += ",\n";
    value += String.format("queryQuorumSize=%d", queryQuorumSize);
    value += ",\n";
    value += String.format("queryQuorumStrategy=%s", queryQuorumStrategy);
    value += ",\n";
    value += String.format("consistentQueryExecution=%s", consistentQueryExecution);
    value += ",\n";
    value += String.format("requireCommandQuorum=%s", requireCommandQuorum);
    value += ",\n";
    value += String.format("commandQuorumSize=%s", commandQuorumSize);
    value += ",\n";
    value += String.format("commandQuorumStrategy=%s", commandQuorumStrategy);
    value += ",\n";
    value += String.format("consistentCommandExecution=%s", consistentCommandExecution);
    value += ",\n";
    value += String.format("maxLogSize=%d", maxLogSize);
    value += ",\n";
    value += String.format("correlationStrategy=%s", correlationStrategy);
    value += ",\n";
    value += String.format("timerStrategy=%s", timerStrategy);
    value += "\n]";
    return value;
  }

}
