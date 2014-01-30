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
package net.kuujo.mimeo;

import net.kuujo.mimeo.cluster.config.ClusterConfig;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

/**
 * A replication service.
 *
 * @author Jordan Halterman
 */
public interface ReplicationService {

  /**
   * Returns the service address.
   *
   * @return
   *   The service address.
   */
  String address();

  /**
   * Returns the replica election timeout.
   *
   * @return
   *   The replica election timeout.
   */
  public long getElectionTimeout();

  /**
   * Sets the leader election timeout.
   *
   * @param timeout
   *   The leader election timeout.
   * @return
   *   The replica configuration.
   */
  public ReplicationService setElectionTimeout(long timeout);

  /**
   * Returns the replica heartbeat interval.
   *
   * @return
   *   The replica heartbeat interval.
   */
  public long getHeartbeatInterval();

  /**
   * Sets the replica heartbeat interval.
   *
   * @param interval
   *   The replica heartbeat interval.
   * @return
   *   The replica configuration.
   */
  public ReplicationService setHeartbeatInterval(long interval);

  /**
   * Returns a boolean indicating whether adaptive timeouts are enabled.
   *
   * @return
   *   Indicates whether adaptive timeouts are enabled.
   */
  public boolean isUseAdaptiveTimeouts();

  /**
   * Indicates whether the replica should use adaptive timeouts.
   *
   * @param useAdaptive
   *   Indicates whether to use adaptive timeouts.
   * @return
   *   The replica configuration.
   */
  public ReplicationService useAdaptiveTimeouts(boolean useAdaptive);

  /**
   * Returns the adaptive timeout threshold.
   *
   * @return
   *   The adaptive timeout threshold.
   */
  public double getAdaptiveTimeoutThreshold();

  /**
   * Sets the adaptive timeout threshold.
   *
   * @param threshold
   *   The adaptive timeout threshold.
   * @return
   *   The replica configuration.
   */
  public ReplicationService setAdaptiveTimeoutThreshold(double threshold);

  /**
   * Returns a boolean indicating whether majority replication is required
   * for write operations.
   *
   * @return
   *   Indicates whether majority replication is required for write operations.
   */
  public boolean isRequireWriteMajority();

  /**
   * Sets whether majority replication is required for write operations.
   *
   * @param require
   *   Indicates whether majority replication should be required for writes.
   * @return
   *   The replication configuration.
   */
  public ReplicationService setRequireWriteMajority(boolean require);

  /**
   * Returns a boolean indicating whether majority synchronization is required
   * for read operations.
   *
   * @return
   *   Indicates whether majority synchronization is required for read operations.
   */
  public boolean isRequireReadMajority();

  /**
   * Sets whether majority synchronization is required for read operations.
   *
   * @param require
   *   Indicates whether majority synchronization should be required for read operations.
   * @return
   *   The replication configuration.
   */
  public ReplicationService setRequireReadMajority(boolean require);

  /**
   * Starts the service.
   *
   * @param config
   *   A cluster configuration.
   * @return
   *   The replication service.
   */
  ReplicationService start(ClusterConfig config);

  /**
   * Starts the service.
   *
   * @param config
   *   A cluster configuration.
   * @param doneHandler
   *   An asynchronous handler to be called once the service has been started.
   * @return
   *   The replication service.
   */
  ReplicationService start(ClusterConfig config, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Registers a state machine command.
   *
   * @param commandName
   *   The command name.
   * @param type
   *   The command type.
   * @param function
   *   The command function.
   * @return
   *   The replication service.
   */
  ReplicationService registerCommand(String commandName, Command.Type type, Function<Command, JsonObject> function);

  /**
   * Unregisters a state machine command.
   *
   * @param commandName
   *   The command name.
   * @return
   *   The replication service.
   */
  ReplicationService unregisterCommand(String commandName);

  /**
   * Submits a command to the replication service.
   *
   * @param command
   *   The command to submit.
   * @param data
   *   The command data.
   * @param doneHandler
   *   An asynchronous handler to be called with the command result.
   * @return
   *   The replication service.
   */
  ReplicationService submitCommand(String command, JsonObject data, Handler<AsyncResult<JsonObject>> doneHandler);

  /**
   * Stops the replication service.
   */
  void stop();

  /**
   * Stops the replication service.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the service is stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
