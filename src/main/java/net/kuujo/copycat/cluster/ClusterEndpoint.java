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
 * A cluster endpoint.
 *
 * @author Jordan Halterman
 */
public interface ClusterEndpoint {

  /**
   * Sets the local endpoint address.
   *
   * @param address
   *   The local endpoint address.
   * @return
   *   The cluster endpoint.
   */
  ClusterEndpoint setLocalAddress(String address);

  /**
   * Returns the local endpoint address.
   *
   * @return
   *   The local endpoint address.
   */
  String getLocalAddress();

  /**
   * Sets the cluster broadcast address.
   *
   * @param address
   *   The cluster broadcast address.
   * @return
   *   The cluster endpoint.
   */
  ClusterEndpoint setBroadcastAddress(String address);

  /**
   * Returns the cluster broadcast address.
   *
   * @return
   *   The cluster broadcast address.
   */
  String getBroadcastAddress();

  /**
   * Starts the endpoint.
   *
   * @return
   *   The endpoint instance.
   */
  ClusterEndpoint start();

  /**
   * Starts the endpoint.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the endpoint has started.
   * @return
   *   The endpoint instance.
   */
  ClusterEndpoint start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Stops the endpoint.
   */
  void stop();

  /**
   * Stops the endpoint.
   *
   * @param doneHandler
   *   An asynchronous handler to be called once the endpoint has stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
