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

import java.util.Set;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * Handles listening for membership changes within a cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
interface ClusterListener {

  /**
   * Gets a set of all members currently in the cluster.
   *
   * @param resultHandler An asynchronous handler to be called with the result.
   */
  void getMembers(Handler<AsyncResult<Set<String>>> resultHandler);

  /**
   * Joins the cluster.
   *
   * @param address The address at which to join the cluster.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void join(String address, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Registers a handler to be called when a node joins the cluster.
   *
   * @param handler The handler to be called when a node joins the cluster.
   */
  void joinHandler(Handler<String> handler);

  /**
   * Leaves the cluster.
   *
   * @param address The address at which to leave the cluster.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void leave(String address, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Registers a handler to be called when a node leaves the cluster.
   *
   * @param handler The handler to be called when a node leaves the cluster.
   */
  void leaveHandler(Handler<String> handler);

}
