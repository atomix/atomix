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
package net.kuujo.copycat.cluster;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * A cluster controller.
 * 
 * @author Jordan Halterman
 */
public interface ClusterController {

  /**
   * Returns the cluster configuration.
   * 
   * @return The cluster configuration.
   */
  ClusterConfig config();

  /**
   * Sets the local endpoint address.
   * 
   * @param address The local endpoint address.
   * @return The cluster controller.
   */
  ClusterController setLocalAddress(String address);

  /**
   * Returns the local endpoint address.
   * 
   * @return The local endpoint address.
   */
  String getLocalAddress();

  /**
   * Sets the cluster broadcast address.
   * 
   * @param address A cluster-wide event bus address.
   * @return The cluster controller.
   */
  ClusterController setBroadcastAddress(String address);

  /**
   * Returns the cluster broadcast address.
   * 
   * @return An event bus address.
   */
  String getBroadcastAddress();

  /**
   * Sets the cluster broadcast interval.
   * 
   * @param interval The interval at which the locator will broadcast to nodes in the
   *          cluster.
   * @return The cluster controller.
   */
  ClusterController setBroadcastInterval(long interval);

  /**
   * Returns the cluster broadcast interval.
   * 
   * @return The interval at which the locator will broadcast to nodes in the cluster.
   */
  long getBroadcastInterval();

  /**
   * Sets the cluster broadcast timeout.
   * 
   * @param timeout A timeout in millisecond within which nodes must respond to the
   *          locator.
   * @return The cluster controller.
   */
  ClusterController setBroadcastTimeout(long timeout);

  /**
   * Returns the cluster broadcast timeout.
   * 
   * @return The cluster broadcast timeout in milliseconds.
   */
  long getBroadcastTimeout();

  /**
   * Starts the cluster controller.
   * 
   * @return The cluster controller.
   */
  ClusterController start();

  /**
   * Starts the cluster controller.
   * 
   * @param doneHandler An asynchronous handler to be called once the controller is
   *          started. The handler will be passed the full detected cluster configuration.
   * @return The cluster controller.
   */
  ClusterController start(Handler<AsyncResult<ClusterConfig>> doneHandler);

  /**
   * Stops the cluster controller.
   */
  void stop();

  /**
   * Stops the cluster controller.
   * 
   * @param doneHandler An asynchronous handler to be called once the controller is
   *          stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
