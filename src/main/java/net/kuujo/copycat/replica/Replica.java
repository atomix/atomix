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
package net.kuujo.copycat.replica;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.state.StateMachine;

/**
 * A replica.
 *
 * @author Jordan Halterman
 */
public interface Replica {

  /**
   * Sets the replica address.
   *
   * @param address
   *   The replica address.
   * @return
   *   The replica instance.
   */
  Replica setAddress(String address);

  /**
   * Returns the replica address.
   *
   * @return
   *   The replica address.
   */
  String getAddress();

  /**
   * Sets the cluster configuration.
   *
   * @param config
   *   A cluster configuration.
   * @return
   *   The replica instance.
   */
  Replica setClusterConfig(ClusterConfig config);

  /**
   * Returns the cluster configuration.
   *
   * @return
   *   The replica's cluster configuration.
   */
  ClusterConfig getClusterConfig();

  /**
   * Sets the election timeout.
   *
   * @param timeout
   *   An election timeout.
   * @return
   *   The replica instance.
   */
  Replica setElectionTimeout(long timeout);

  /**
   * Returns the election timeout.
   *
   * @return
   *   The replica's election timeout.
   */
  long getElectionTimeout();

  /**
   * Sets the heartbeat interval.
   *
   * @param interval
   *   A heartbeat interval, in milliseconds.
   * @return
   *   The replica instance.
   */
  Replica setHeartbeatInterval(long interval);

  /**
   * Returns the replica's heartbeat interval.
   *
   * @return
   *   The replica's heartbeat interval.
   */
  long getHeartbeatInterval();

  /**
   * Returns a boolean indicating whether adaptive timeouts are enabled.
   * 
   * @return Indicates whether adaptive timeouts are enabled.
   */
  boolean isUseAdaptiveTimeouts();

  /**
   * Indicates whether the replica should use adaptive timeouts.
   * 
   * @param useAdaptive Indicates whether to use adaptive timeouts.
   * @return The replica.
   */
  Replica useAdaptiveTimeouts(boolean useAdaptive);

  /**
   * Returns the adaptive timeout threshold.
   * 
   * @return The adaptive timeout threshold.
   */
  double getAdaptiveTimeoutThreshold();

  /**
   * Sets the adaptive timeout threshold.
   * 
   * @param threshold The adaptive timeout threshold.
   * @return The replica.
   */
  Replica setAdaptiveTimeoutThreshold(double threshold);

  /**
   * Returns a boolean indicating whether majority replication is required for
   * write operations.
   * 
   * @return Indicates whether majority replication is required for write
   *         operations.
   */
  boolean isRequireWriteMajority();

  /**
   * Sets whether majority replication is required for write operations.
   * 
   * @param require Indicates whether majority replication should be required
   *          for writes.
   * @return The replica.
   */
  Replica setRequireWriteMajority(boolean require);

  /**
   * Returns a boolean indicating whether majority synchronization is required
   * for read operations.
   * 
   * @return Indicates whether majority synchronization is required for read
   *         operations.
   */
  boolean isRequireReadMajority();

  /**
   * Sets whether majority synchronization is required for read operations.
   * 
   * @param require Indicates whether majority synchronization should be
   *          required for read operations.
   * @return The replica.
   */
  Replica setRequireReadMajority(boolean require);

  /**
   * Sets the state machine.
   *
   * @param stateMachine
   *   The replica's state machine.
   * @return
   *   The replica instance.
   */
  Replica setStateMachine(StateMachine stateMachine);

  /**
   * Returns the replica's state machine.
   *
   * @return
   *   The replica's state machine.
   */
  StateMachine getStateMachine();

  /**
   * Sets the replicated log.
   *
   * @param log
   *   A replicated log.
   * @return
   *   The replica instance.
   */
  Replica setLog(Log log);

  /**
   * Returns the replica's log.
   *
   * @return
   *   The replica's replicated log.
   */
  Log getLog();

  /**
   * Starts the replica.
   *
   * @return
   *   The replica instance.
   */
  Replica start();

  /**
   * Starts the replica.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the replica has started.
   * @return
   *   The replica instance.
   */
  Replica start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Submits a command to the replica.
   *
   * @param command
   *   The command to submit.
   * @param doneHandler
   *   An asynchronous handler to be called with the command result.
   * @return
   *   The replica instance.
   */
  <R> Replica submitCommand(Command command, Handler<AsyncResult<R>> doneHandler);

  /**
   * Stops the replica.
   */
  void stop();

  /**
   * Stops the replica.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the replica has stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
