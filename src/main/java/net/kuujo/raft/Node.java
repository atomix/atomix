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

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * A Via node.
 *
 * @author Jordan Halterman
 */
public interface Node {

  /**
   * Returns the cluster address.
   *
   * @return
   *   The cluster address.
   */
  String cluster();

  /**
   * Returns the node address.
   *
   * @return
   *   The node address.
   */
  String address();

  /**
   * Sets the node election timeout.
   *
   * @param timeout
   *   The node election timeout.
   * @return
   *   The node instance.
   */
  Node setElectionTimeout(long timeout);

  /**
   * Gets the node election timeout.
   *
   * @return
   *   The node election timeout.
   */
  long getElectionTimeout();

  /**
   * Sets the node heartbeat interval.
   *
   * @param interval
   *   The node heartbeat interval.
   * @return
   *   The node instance.
   */
  Node setHeartbeatInterval(long interval);

  /**
   * Gets the node heartbeat interval.
   *
   * @return
   *   The node heartbeat interval.
   */
  long getHeartbeatInterval();

  /**
   * Sets the node response threshold.
   *
   * @param threshold
   *   The node response threshold.
   * @return
   *   The node instance.
   */
  Node setResponseThreshold(double threshold);

  /**
   * Gets the node response threshold.
   *
   * @return
   *   The node response threshold.
   */
  double getResponseThreshold();

  /**
   * Starts the node.
   *
   * @return
   *   The node instance.
   */
  Node start();

  /**
   * Starts the node.
   *
   * @param doneHandler
   *   An asychronous handler to be called once the node is started.
   * @return
   *   The node instance.
   */
  Node start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Stops the node.
   */
  void stop();

  /**
   * Stops the node.
   *
   * @param doneHandler
   *   An asynchronous handler to be called when the node is stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
