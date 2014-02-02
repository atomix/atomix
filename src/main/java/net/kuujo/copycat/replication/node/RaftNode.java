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
package net.kuujo.copycat.replication.node;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

import net.kuujo.copycat.Command;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.replication.StateMachine;

/**
 * A Raft protocol node.
 * 
 * @author Jordan Halterman
 */
public interface RaftNode {

  /**
   * Sets the node address.
   * 
   * @param address The node address.
   * @return The node instance.
   */
  RaftNode setAddress(String address);

  /**
   * Returns the node address.
   * 
   * @return The node address.
   */
  String getAddress();

  /**
   * Sets the cluster configuration.
   * 
   * @param config A cluster configuration.
   * @return The node instance.
   */
  RaftNode setClusterConfig(ClusterConfig config);

  /**
   * Returns the cluster configuration.
   * 
   * @return The node's cluster configuration.
   */
  ClusterConfig getClusterConfig();

  /**
   * Sets the election timeout.
   * 
   * @param timeout An election timeout.
   * @return The node instance.
   */
  RaftNode setElectionTimeout(long timeout);

  /**
   * Returns the election timeout.
   * 
   * @return The node's election timeout.
   */
  long getElectionTimeout();

  /**
   * Sets the heartbeat interval.
   * 
   * @param interval A heartbeat interval, in milliseconds.
   * @return The node instance.
   */
  RaftNode setHeartbeatInterval(long interval);

  /**
   * Returns the node's heartbeat interval.
   * 
   * @return The node's heartbeat interval.
   */
  long getHeartbeatInterval();

  /**
   * Returns a boolean indicating whether adaptive timeouts are enabled.
   * 
   * @return Indicates whether adaptive timeouts are enabled.
   */
  boolean isUseAdaptiveTimeouts();

  /**
   * Indicates whether the node should use adaptive timeouts.
   * 
   * @param useAdaptive Indicates whether to use adaptive timeouts.
   * @return The node.
   */
  RaftNode useAdaptiveTimeouts(boolean useAdaptive);

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
   * @return The node.
   */
  RaftNode setAdaptiveTimeoutThreshold(double threshold);

  /**
   * Returns a boolean indicating whether majority nodetion is required for write
   * operations.
   * 
   * @return Indicates whether majority nodetion is required for write operations.
   */
  boolean isRequireWriteMajority();

  /**
   * Sets whether majority nodetion is required for write operations.
   * 
   * @param require Indicates whether majority nodetion should be required for writes.
   * @return The node.
   */
  RaftNode setRequireWriteMajority(boolean require);

  /**
   * Returns a boolean indicating whether majority synchronization is required for read
   * operations.
   * 
   * @return Indicates whether majority synchronization is required for read operations.
   */
  boolean isRequireReadMajority();

  /**
   * Sets whether majority synchronization is required for read operations.
   * 
   * @param require Indicates whether majority synchronization should be required for read
   *          operations.
   * @return The node.
   */
  RaftNode setRequireReadMajority(boolean require);

  /**
   * Sets the state machine.
   * 
   * @param stateMachine The node's state machine.
   * @return The node instance.
   */
  RaftNode setStateMachine(StateMachine stateMachine);

  /**
   * Returns the node's state machine.
   * 
   * @return The node's state machine.
   */
  StateMachine getStateMachine();

  /**
   * Sets the nodeted log.
   * 
   * @param log A nodeted log.
   * @return The node instance.
   */
  RaftNode setLog(Log log);

  /**
   * Returns the node's log.
   * 
   * @return The node's nodeted log.
   */
  Log getLog();

  /**
   * Starts the node.
   * 
   * @return The node instance.
   */
  RaftNode start();

  /**
   * Starts the node.
   * 
   * @param doneHandler An asynchronous handler to be called once the node has started.
   * @return The node instance.
   */
  RaftNode start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Submits a command to the node.
   * 
   * @param command The command to submit.
   * @param doneHandler An asynchronous handler to be called with the command result.
   * @return The node instance.
   */
  <R> RaftNode submitCommand(Command command, Handler<AsyncResult<R>> doneHandler);

  /**
   * Stops the node.
   */
  void stop();

  /**
   * Stops the node.
   * 
   * @param doneHandler An asynchronous handler to be called once the node has stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
