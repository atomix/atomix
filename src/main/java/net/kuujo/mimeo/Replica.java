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

import net.kuujo.mimeo.cluster.ClusterController;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * A replica.
 * 
 * @author Jordan Halterman
 */
public interface Replica {

  /**
   * Returns the address of the replica.
   * 
   * @return The unique event bus address of the replica.
   */
  String address();

  /**
   * Returns the cluster handler for the replica.
   * 
   * @return The replica's cluster handler.
   */
  ClusterController cluster();

  /**
   * Returns the replication service.
   * 
   * @return The replication service.
   */
  ReplicationService service();

  /**
   * Starts the replica.
   * 
   * @return The replica instance.
   */
  Replica start();

  /**
   * Starts the replica.
   * 
   * @param doneHandler An asynchronous handler to be called once the replica is
   *          started.
   * @return The replica instance.
   */
  Replica start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Stops the replica.
   */
  void stop();

  /**
   * Stops the replica.
   * 
   * @param doneHandler An asynchronous handler to be called once the replica is
   *          stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
