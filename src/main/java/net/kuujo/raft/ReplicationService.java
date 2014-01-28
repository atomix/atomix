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
package net.kuujo.raft;

import java.util.Set;

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
   * Sets the service election timeout.
   *
   * @param timeout
   *   The service election timeout.
   * @return
   *   The replication service.
   */
  ReplicationService setElectionTimeout(long timeout);

  /**
   * Gets the service election timeout.
   *
   * @return
   *   The service election timeout.
   */
  long getElectionTimeout();

  /**
   * Starts the service.
   *
   * @return
   *   The replication service.
   */
  ReplicationService start();

  /**
   * Starts the service.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the service has been started.
   * @return
   *   The replication service.
   */
  ReplicationService start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Returns a boolean indicating whether an address is a member of the cluster.
   *
   * @param address
   *   The address to check.
   * @return
   *   Indicates whether the address is a member of the cluster.
   */
  boolean hasMember(String address);

  /**
   * Adds a member to the cluster.
   *
   * @param address
   *   The member address.
   * @return
   *   The replication service.
   */
  ReplicationService addMember(String address);

  /**
   * Removes a member from the cluster.
   *
   * @param address
   *   The member address.
   * @return
   *   The replication service.
   */
  ReplicationService removeMember(String address);

  /**
   * Sets cluster members.
   *
   * @param addresses
   *   A list of cluster addresses.
   * @return
   *   The replication service.
   */
  public ReplicationService setMembers(String... addresses);

  /**
   * Sets cluster members.
   *
   * @param addresses
   *   A set of cluster addresses.
   * @return
   *   The replication service.
   */
  public ReplicationService setMembers(Set<String> addresses);

  /**
   * Returns a set of current known cluster members.
   *
   * @return
   *   A set of current cluster members.
   */
  public Set<String> members();

  /**
   * Registers a state machine command.
   *
   * @param command
   *   The command to register.
   * @param type
   *   The command type.
   * @return
   *   The replication service.
   */
  ReplicationService registerCommand(String command, Command.Type type, Function<Command, JsonObject> function);

  /**
   * Unregisters a state machine command.
   *
   * @param command
   *   The command to unregister.
   * @return
   *   The replication service.
   */
  ReplicationService unregisterCommand(String command);

  /**
   * Submits a command to the replication service.
   *
   * @param command
   *   The command to submit.
   * @param data
   *   Command arguments.
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
