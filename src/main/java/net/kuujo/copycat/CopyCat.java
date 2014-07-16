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

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ClusterManager;
import net.kuujo.copycat.cluster.StaticClusterManager;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.MemoryLog;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonObject;

/**
 * Vert.x friendly CopyCat API.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopyCat {
  final Vertx vertx;
  private final CommandRegistry registry = new CommandRegistry();
  private final StateMachine stateMachine = new RegistryAssistedStateMachine(registry);
  private final ClusterManager cluster;
  private final CopyCatConfig config = new CopyCatConfig();
  private final CopyCatContext context;
  private boolean started;

  public CopyCat(Vertx vertx) {
    this(vertx, new MemoryLog(), new StaticClusterManager());
  }

  public CopyCat(Vertx vertx, Log log) {
    this(vertx, log, new StaticClusterManager());
  }

  public CopyCat(Vertx vertx, ClusterManager cluster) {
    this(vertx, new MemoryLog(), cluster);
  }

  public CopyCat(Vertx vertx, Log log, ClusterManager cluster) {
    this.vertx = vertx;
    this.cluster = cluster;
    this.context = new CopyCatContext(vertx, stateMachine, log, cluster.config(), new CopyCatConfig());
  }

  public CopyCat(Vertx vertx, ClusterConfig cluster) {
    this(vertx, new MemoryLog(), cluster);
  }

  public CopyCat(Vertx vertx, Log log, ClusterConfig cluster) {
    this.vertx = vertx;
    this.cluster = new StaticClusterManager(cluster);
    this.context = new CopyCatContext(vertx, stateMachine, log, cluster, new CopyCatConfig());
  }

  /**
   * Returns the underlying copycat context.
   *
   * @return The underlying copycat context.
   */
  public CopyCatContext context() {
    return context;
  }

  /**
   * Registers a state machine command.<p>
   *
   * All state machine commands should be designed such that given the same
   * input in the same order they should always produce the same output. That
   * means commands should not be accessing external resources (such as databases)
   * in which state may differ over time. Rather, the state machine's internal
   * state should be the only variables impacting command output.
   *
   * @param name The name of the command to register.
   * @param command The command to register.
   * @return The copycat instance.
   */
  public CopyCat registerCommand(String name, Command<JsonObject, JsonObject> command) {
    registry.registerCommand(name, command);
    return this;
  }

  /**
   * Registers a typed state machine command.<p>
   *
   * All state machine commands should be designed such that given the same
   * input in the same order they should always produce the same output. That
   * means commands should not be accessing external resources (such as databases)
   * in which state may differ over time. Rather, the state machine's internal
   * state should be the only variables impacting command output.<p>
   *
   * Registering state machine commands using command types can help improve
   * performance by allowing CopyCat to reason about how state machine state
   * is being modified. For <code>READ<code> commands, CopyCat does not have to
   * replicate the log and instead can simply ping nodes in order to ensure that
   * logs are in sync during read operations.
   *
   * @param name The name of the command to register.
   * @param type The type of the command to register.
   * @param command The command to register.
   * @return The copycat instance.
   */
  public CopyCat registerCommand(String name, CommandInfo.Type type, Command<JsonObject, JsonObject> command) {
    registry.registerCommand(name, type, command);
    return this;
  }

  /**
   * Registers a read-only state machine command.<p>
   *
   * All state machine commands should be designed such that given the same
   * input in the same order they should always produce the same output. That
   * means commands should not be accessing external resources (such as databases)
   * in which state may differ over time. Rather, the state machine's internal
   * state should be the only variables impacting command output.<p>
   *
   * Registering state machine commands using command types can help improve
   * performance by allowing CopyCat to reason about how state machine state
   * is being modified. For <code>READ<code> commands, CopyCat does not have to
   * replicate the log and instead can simply ping nodes in order to ensure that
   * logs are in sync during read operations.
   *
   * @param name The name of the command to register.
   * @param command The command to register.
   * @return The copycat instance.
   */
  public CopyCat registerReadCommand(String name, Command<JsonObject, JsonObject> command) {
    return registerCommand(name, CommandInfo.Type.READ, command);
  }

  /**
   * Registers a write-only state machine command.<p>
   *
   * All state machine commands should be designed such that given the same
   * input in the same order they should always produce the same output. That
   * means commands should not be accessing external resources (such as databases)
   * in which state may differ over time. Rather, the state machine's internal
   * state should be the only variables impacting command output.<p>
   *
   * Registering state machine commands using command types can help improve
   * performance by allowing CopyCat to reason about how state machine state
   * is being modified. For <code>READ<code> commands, CopyCat does not have to
   * replicate the log and instead can simply ping nodes in order to ensure that
   * logs are in sync during read operations.
   *
   * @param name The name of the command to register.
   * @param command The command to register.
   * @return The copycat instance.
   */
  public CopyCat registerWriteCommand(String name, Command<JsonObject, JsonObject> command) {
    return registerCommand(name, CommandInfo.Type.WRITE, command);
  }

  /**
   * Unregisters a state machine command.
   *
   * @param name The name of the command to unregister.
   * @return The copycat instance.
   */
  public CopyCat unregisterCommand(String name) {
    registry.unregisterCommand(name);
    return this;
  }

  /**
   * Registers a snapshot creator.<p>
   *
   * The snapshot creator will be called whenever the local replicated log
   * becomes full. When the log becomes full, the snapshot creator will be
   * called to get a snapshot of the current machine state. The snapshotted
   * state will then be logged to the local log, and all previous entries in
   * the log will be erased.
   *
   * @param creator The snapshot creator to register.
   * @return The copycat instance.
   */
  public CopyCat snapshotCreator(SnapshotCreator creator) {
    registry.snapshotCreator(creator);
    return this;
  }

  /**
   * Registers a snapshot installer.<p>
   *
   * The snapshot installer will be called when the {@link CopyCatContext}
   * first starts up. The current machine state will be read from the local
   * log, and the snapshot installer will be called in order to install
   * the machine state.
   *
   * @param installer The snapshot installer.
   * @return The copycat instance.
   */
  public CopyCat snapshotInstaller(SnapshotInstaller installer) {
    registry.snapshotInstaller(installer);
    return this;
  }

  /**
   * Sets the replica election timeout.<p>
   *
   * If the replica does not hear from any other members of the cluster within
   * the given timeout, the replica will begin a new election. Similarly, if
   * a quorum of the nodes do not respond within the given timeout during an
   * election, the election is restarted. Note that CopyCat will slightly
   * vary timeouts in order to decrease the probability that two nodes will
   * begin an election at the same time.
   * 
   * @param timeout The election timeout.
   * @return The copycat instance.
   */
  public CopyCat setElectionTimeout(long timeout) {
    checkNotStarted();
    config.setElectionTimeout(timeout);
    return this;
  }

  /**
   * Returns the replica election timeout.
   * 
   * @return The election timeout.
   */
  public long getElectionTimeout() {
    return config.getElectionTimeout();
  }

  /**
   * Sets the replica heartbeat interval.<p>
   *
   * This is the interval at which the leader of the cluster will send heartbeat
   * messages to followers. The heartbeat interval should be an order of magnitude
   * less than the election timeout in order to ensure that timeouts do not occur
   * due to a temporarily blocked event loop.
   * 
   * @param interval The interval at which the node should send heartbeat messages.
   * @return The copycat instance.
   */
  public CopyCat setHeartbeatInterval(long interval) {
    checkNotStarted();
    config.setHeartbeatInterval(interval);
    return this;
  }

  /**
   * Returns the replica heartbeat interval.
   * 
   * @return The replica heartbeat interval.
   */
  public long getHeartbeatInterval() {
    return config.getHeartbeatInterval();
  }

  /**
   * Returns a boolean indicating whether adaptive timeouts are enabled.
   * 
   * @return Indicates whether adaptive timeouts are enabled.
   */
  public boolean isUseAdaptiveTimeouts() {
    return config.isUseAdaptiveTimeouts();
  }

  /**
   * Indicates whether the replica should use adaptive timeouts.<p>
   *
   * CopyCat can use adaptive timeouts for pinging nodes in the cluster. When
   * using adaptive timeouts, the leader will alter expected timeouts based
   * on the last response time of each node. This allows the cluster to detect
   * and resolve failures more quickly. By default this feature is disabled.
   * 
   * @param useAdaptive Indicates whether to use adaptive timeouts.
   * @return The copycat instance.
   */
  public CopyCat setUseAdaptiveTimeouts(boolean useAdaptive) {
    checkNotStarted();
    config.setUseAdaptiveTimeouts(useAdaptive);
    return this;
  }

  /**
   * Returns the adaptive timeout threshold.
   * 
   * @return The adaptive timeout threshold.
   */
  public double getAdaptiveTimeoutThreshold() {
    return config.getAdaptiveTimeoutThreshold();
  }

  /**
   * Sets the adaptive timeout threshold.<p>
   *
   * The adaptive timeout threshold is essentially a floor for adaptive timeouts.
   * If ping timeouts are occurring frequently with adaptive timeouts, increase the
   * adaptive timeout threshold.
   * 
   * @param threshold The adaptive timeout threshold.
   * @return The copycat instance.
   */
  public CopyCat setAdaptiveTimeoutThreshold(double threshold) {
    checkNotStarted();
    config.setAdaptiveTimeoutThreshold(threshold);
    return this;
  }

  /**
   * Returns a boolean indicating whether a quorum replication is required for write
   * operations.
   * 
   * @return Indicates whether a quorum replication is required for write operations.
   */
  public boolean isRequireWriteQuorum() {
    return config.isRequireWriteQuorum();
  }

  /**
   * Sets whether a quorum replication is required for write operations.<p>
   *
   * When executing a write command, enabling write quorums will cause the leader to
   * replica the log entry to a quorum of nodes in the cluster prior to responding to
   * the submit request. This feature is enabled by default.
   * 
   * @param require Indicates whether a quorum replication should be required for writes.
   * @return The copycat instance.
   */
  public CopyCat setRequireWriteQuorum(boolean require) {
    checkNotStarted();
    config.setRequireWriteQuorum(require);
    return this;
  }

  /**
   * Returns a boolean indicating whether a quorum synchronization is required for read
   * operations.
   * 
   * @return Indicates whether a quorum synchronization is required for read operations.
   */
  public boolean isRequireReadQuorum() {
    return config.isRequireReadQuorum();
  }

  /**
   * Sets whether a quorum synchronization is required for read operations.<p>
   *
   * When executing a read-only command, enabling read quorums will cause the leader to
   * ping a quorum of the cluster prior to executing the command. This allows the leader
   * to ensure logs are in sync prior to responding. Disabling read quorums introduces the
   * possibility of reading stale data. By default read quorums are enabled.
   * 
   * @param require Indicates whether a quorum synchronization should be required for read
   *          operations.
   * @return The copycat instance.
   */
  public CopyCat setRequireReadQuorum(boolean require) {
    checkNotStarted();
    config.setRequireReadQuorum(require);
    return this;
  }

  /**
   * Start the copycat replica.
   *
   * @return The copycat instance.
   */
  public CopyCat start() {
    return start(null);
  }

  /**
   * Starts the copycat replica.
   *
   * @param doneHandler An asynchronous handler to be called once the replica has
   *        been started.
   * @return The copycat instance.
   */
  public CopyCat start(final Handler<AsyncResult<Void>> doneHandler) {
    cluster.start(new Handler<AsyncResult<ClusterConfig>>() {
      @Override
      public void handle(AsyncResult<ClusterConfig> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          context.start(new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> result) {
              if (result.failed()) {
                cluster.stop();
                new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
              } else {
                started = true;
                new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
              }
            }
          });
        }
      }
    });
    return this;
  }

  /**
   * Stops the copycat replica.
   */
  public void stop() {
    stop(null);
  }

  /**
   * Stops the copycat replica.
   *
   * @param doneHandler An asynchronous handler to be called once the replica has
   *        been stopped.
   */
  public void stop(final Handler<AsyncResult<Void>> doneHandler) {
    checkStarted();
    context.stop(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        cluster.stop(doneHandler);
      }
    });
  }

  /**
   * Checks whether the copycat has been started.
   */
  private void checkStarted() {
    if (!started) {
      throw new IllegalStateException("State machine not started");
    }
  }

  /**
   * Checks whether the copycat has not been started.
   */
  private void checkNotStarted() {
    if (started) {
      throw new IllegalStateException("State machine already started");
    }
  }

  /**
   * Submits a command to the copycat cluster.<p>
   *
   * The given command name should be the name of a registered state machine command.
   * When the command is submitted, the request will first be forwarded to the cluster
   * leader. If no leader is currently available then the command will fail. Once the
   * leader receives the command request, depending on the command type (read or write)
   * the leader may log and replicate the command, and once the entry is committed,
   * apply the command to the leader's state machine and return the result.
   *
   * @param command The name of the command to submit.
   * @param args The command arguments.
   * @param resultHandler An asynchronous handler to be called with the command result.
   * @return The copycat instance.
   */
  public CopyCat submitCommand(String command, JsonObject args, Handler<AsyncResult<JsonObject>> resultHandler) {
    checkStarted();
    context.submitCommand(command, args, resultHandler);
    return this;
  }

}
