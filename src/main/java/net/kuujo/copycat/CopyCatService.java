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

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * A Mimeo service.
 *
 * @author Jordan Halterman
 */
public interface CopyCatService {

  /**
   * Sets the local node address.
   *
   * @param address
   *   A unique event bus address for the local node.
   * @return
   *   The service instance.
   */
  CopyCatService setLocalAddress(String address);

  /**
   * Returns the local node address.
   *
   * @return
   *   The local node address.
   */
  String getLocalAddress();

  /**
   * Sets the service API address.
   *
   * @param address
   *   An event bus address for the service.
   * @return
   *   The service instance.
   */
  CopyCatService setServiceAddress(String address);

  /**
   * Returns the service API address.
   *
   * @return
   *   An event bus address for the service.
   */
  String getServiceAddress();

  /**
   * Sets the cluster broadcast address.
   *
   * @param address
   *   An event bus address at which to search for other nodes in the cluster.
   * @return
   *   The service instance.
   */
  CopyCatService setBroadcastAddress(String address);

  /**
   * Returns the cluster broadcast address.
   *
   * @return
   *   The cluster broadcast address.
   */
  String getBroadcastAddress();

  /**
   * Sets the replica election timeout.
   *
   * @param timeout
   *   The Raft consensus algorithm's election timeout.
   * @return
   *   The service instance.
   */
  CopyCatService setElectionTimeout(long timeout);

  /**
   * Returns the service election timeout.
   *
   * @return
   *   The service election timeout.
   */
  long getElectionTimeout();

  /**
   * Sets the service heartbeat interval.
   *
   * @param interval
   *   The interval at which the service should send heartbeats to other nodes.
   * @return
   *   The service instance.
   */
  CopyCatService setHeartbeatInterval(long interval);

  /**
   * Returns the service heartbeat interval.
   *
   * @return
   *   The interval at which the service will send heartbeats to other nodes.
   */
  long getHeartbeatInterval();

  /**
   * Returns a boolean indicating whether adaptive timeouts are enabled.
   * 
   * @return Indicates whether adaptive timeouts are enabled.
   */
  public boolean isUseAdaptiveTimeouts();

  /**
   * Indicates whether the replica should use adaptive timeouts.
   * 
   * @param useAdaptive Indicates whether to use adaptive timeouts.
   * @return The service.
   */
  public CopyCatService useAdaptiveTimeouts(boolean useAdaptive);

  /**
   * Returns the adaptive timeout threshold.
   * 
   * @return The adaptive timeout threshold.
   */
  public double getAdaptiveTimeoutThreshold();

  /**
   * Sets the adaptive timeout threshold.
   * 
   * @param threshold The adaptive timeout threshold.
   * @return The service.
   */
  public CopyCatService setAdaptiveTimeoutThreshold(double threshold);

  /**
   * Returns a boolean indicating whether majority replication is required for
   * write operations.
   * 
   * @return Indicates whether majority replication is required for write
   *         operations.
   */
  public boolean isRequireWriteMajority();

  /**
   * Sets whether majority replication is required for write operations.
   * 
   * @param require Indicates whether majority replication should be required
   *          for writes.
   * @return The service.
   */
  public CopyCatService setRequireWriteMajority(boolean require);

  /**
   * Returns a boolean indicating whether majority synchronization is required
   * for read operations.
   * 
   * @return Indicates whether majority synchronization is required for read
   *         operations.
   */
  public boolean isRequireReadMajority();

  /**
   * Sets whether majority synchronization is required for read operations.
   * 
   * @param require Indicates whether majority synchronization should be
   *          required for read operations.
   * @return The service.
   */
  public CopyCatService setRequireReadMajority(boolean require);

  /**
   * Registers a state machine command.
   *
   * @param commandName
   *   The command name.
   * @param function
   *   The command function.
   * @return
   *   The service instance.
   */
  <R> CopyCatService registerCommand(String commandName, Function<Command, R> function);

  /**
   * Registers a typed state machine command.
   *
   * @param commandName
   *   The command name.
   * @param type
   *   The command type.
   * @param function
   *   The command function.
   * @return
   *   The service instance.
   */
  <R> CopyCatService registerCommand(String commandName, Command.Type type, Function<Command, R> function);

  /**
   * Unregisters a state machine command.
   *
   * @param commandName
   *   The command name.
   * @return
   *   The service instance.
   */
  CopyCatService unregisterCommand(String commandName);

  /**
   * Starts the service.
   *
   * @return
   *   The service instance.
   */
  CopyCatService start();

  /**
   * Starts the service.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the service is started.
   * @return
   *   The service instance.
   */
  CopyCatService start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Stops the service.
   *
   * @return
   *   The service instance.
   */
  void stop();

  /**
   * Stops the service.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the service is stopped.
   * @return
   *   The service instance.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
