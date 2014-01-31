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
package net.kuujo.mimeo.replica;

import net.kuujo.mimeo.protocol.PingRequest;
import net.kuujo.mimeo.protocol.PingResponse;
import net.kuujo.mimeo.protocol.PollRequest;
import net.kuujo.mimeo.protocol.PollResponse;
import net.kuujo.mimeo.protocol.SubmitRequest;
import net.kuujo.mimeo.protocol.SubmitResponse;
import net.kuujo.mimeo.protocol.SyncRequest;
import net.kuujo.mimeo.protocol.SyncResponse;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * A replica endpoint.
 * 
 * @author Jordan Halterman
 */
public interface ReplicaEndpoint {

  /**
   * Sets the service address.
   *
   * @param address
   *   The service address.
   * @return
   *   The replica endpoint.
   */
  ReplicaEndpoint setAddress(String address);

  /**
   * Returns the service address.
   * 
   * @return The service address.
   */
  String getAddress();

  /**
   * Sends a ping request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param resultHandler An asynchronous handler to be called with the ping
   *          response.
   * @return The replica endpoint.
   */
  ReplicaEndpoint ping(String address, PingRequest request, Handler<AsyncResult<PingResponse>> resultHandler);

  /**
   * Sends a ping request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the ping
   *          response.
   * @return The replica endpoint.
   */
  ReplicaEndpoint ping(String address, PingRequest request, long timeout, Handler<AsyncResult<PingResponse>> resultHandler);

  /**
   * Registers a ping request handler.
   * 
   * @param handler A ping request handler.
   * @return The replica endpoint.
   */
  ReplicaEndpoint pingHandler(Handler<PingRequest> handler);

  /**
   * Sends a sync request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param resultHandler An asynchronous handler to be called with the sync
   *          response.
   * @return The replica endpoint.
   */
  ReplicaEndpoint sync(String address, SyncRequest request, Handler<AsyncResult<SyncResponse>> resultHandler);

  /**
   * Sends a sync request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the sync
   *          response.
   * @return The replica endpoint.
   */
  ReplicaEndpoint sync(String address, SyncRequest request, long timeout, Handler<AsyncResult<SyncResponse>> resultHandler);

  /**
   * Registers async request handler.
   * 
   * @param handler An append entries request handler.
   * @return The replica endpoint.
   */
  ReplicaEndpoint syncHandler(Handler<SyncRequest> handler);

  /**
   * Sends a poll request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param resultHandler An asynchronous handler to be called with the poll
   *          response.
   * @return The replica endpoint.
   */
  ReplicaEndpoint poll(String address, PollRequest request, Handler<AsyncResult<PollResponse>> resultHandler);

  /**
   * Sends a poll request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the poll
   *          response.
   * @return The replica endpoint.
   */
  ReplicaEndpoint poll(String address, PollRequest request, long timeout, Handler<AsyncResult<PollResponse>> resultHandler);

  /**
   * Registers a poll request handler.
   * 
   * @param handler A poll request handler.
   * @return The replica endpoint.
   */
  ReplicaEndpoint pollHandler(Handler<PollRequest> handler);

  /**
   * Sends a submit request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param resultHandler An asynchronous handler to be called with the submit
   *          response.
   * @return The replica endpoint.
   */
  ReplicaEndpoint submit(String address, SubmitRequest request, Handler<AsyncResult<SubmitResponse>> resultHandler);

  /**
   * Sends a submit request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the submit
   *          response.
   * @return The replica endpoint.
   */
  ReplicaEndpoint submit(String address, SubmitRequest request, long timeout, Handler<AsyncResult<SubmitResponse>> resultHandler);

  /**
   * Registers a submit request handler.
   * 
   * @param handler A submit request handler.
   * @return The replica endpoint.
   */
  ReplicaEndpoint submitHandler(Handler<SubmitRequest> handler);

  /**
   * Starts the replica endpoint.
   * 
   * @return The replica endpoint.
   */
  ReplicaEndpoint start();

  /**
   * Starts the replica endpoint.
   * 
   * @param doneHandler An asynchronous handler to be called once the endpoint
   *          is started.
   * @return The replica endpoint.
   */
  ReplicaEndpoint start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Stops the replica endpoint.
   */
  void stop();

  /**
   * Stops the replica endpoint.
   * 
   * @param doneHandler An asynchronous handler to be called once the endpoint
   *          is stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
