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
 * A cluster locator.
 * 
 * @author Jordan Halterman
 */
public interface ClusterLocator {

  /**
   * Returns the cluster configuration.
   * 
   * @return The cluster configuration.
   */
  ClusterConfig config();

  /**
   * Sets the cluster broadcast address.
   * 
   * @param address A cluster-wide event bus address.
   * @return The cluster locator.
   */
  ClusterLocator setBroadcastAddress(String address);

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
   * @return The cluster locator.
   */
  ClusterLocator setBroadcastInterval(long interval);

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
   * @return The cluster locator.
   */
  ClusterLocator setBroadcastTimeout(long timeout);

  /**
   * Returns the cluster broadcast timeout.
   * 
   * @return The cluster broadcast timeout in milliseconds.
   */
  long getBroadcastTimeout();

  /**
   * Starts the cluster locator.
   * 
   * @return The cluster locator.
   */
  ClusterLocator start();

  /**
   * Starts the cluster locator.
   * 
   * @param doneHandler An asynchronous handler to be called once the locator is started.
   *          The handler will be passed the full detected cluster configuration.
   * @return The cluster locator.
   */
  ClusterLocator start(Handler<AsyncResult<ClusterConfig>> doneHandler);

  /**
   * Stops the cluster locator.
   */
  void stop();

  /**
   * Stops the cluster locator.
   * 
   * @param doneHandler An asynchronous handler to be called once the locator is stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
