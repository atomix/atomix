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

import net.kuujo.copycat.replication.protocol.PingRequest;
import net.kuujo.copycat.replication.protocol.PingResponse;
import net.kuujo.copycat.replication.protocol.PollRequest;
import net.kuujo.copycat.replication.protocol.PollResponse;
import net.kuujo.copycat.replication.protocol.SubmitRequest;
import net.kuujo.copycat.replication.protocol.SubmitResponse;
import net.kuujo.copycat.replication.protocol.SyncRequest;
import net.kuujo.copycat.replication.protocol.SyncResponse;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

/**
 * A Raft protocol client.
 * 
 * @author Jordan Halterman
 */
public interface RaftClient {

  /**
   * Sets the service address.
   * 
   * @param address The service address.
   * @return The client.
   */
  RaftClient setAddress(String address);

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
   * @param resultHandler An asynchronous handler to be called with the ping response.
   * @return The client.
   */
  RaftClient ping(String address, PingRequest request, Handler<AsyncResult<PingResponse>> resultHandler);

  /**
   * Sends a ping request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the ping response.
   * @return The client.
   */
  RaftClient ping(String address, PingRequest request, long timeout, Handler<AsyncResult<PingResponse>> resultHandler);

  /**
   * Registers a ping request handler.
   * 
   * @param handler A ping request handler.
   * @return The client.
   */
  RaftClient pingHandler(Handler<PingRequest> handler);

  /**
   * Sends a sync request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param resultHandler An asynchronous handler to be called with the sync response.
   * @return The client.
   */
  RaftClient sync(String address, SyncRequest request, Handler<AsyncResult<SyncResponse>> resultHandler);

  /**
   * Sends a sync request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the sync response.
   * @return The client.
   */
  RaftClient sync(String address, SyncRequest request, long timeout, Handler<AsyncResult<SyncResponse>> resultHandler);

  /**
   * Registers async request handler.
   * 
   * @param handler An append entries request handler.
   * @return The client.
   */
  RaftClient syncHandler(Handler<SyncRequest> handler);

  /**
   * Sends a poll request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param resultHandler An asynchronous handler to be called with the poll response.
   * @return The client.
   */
  RaftClient poll(String address, PollRequest request, Handler<AsyncResult<PollResponse>> resultHandler);

  /**
   * Sends a poll request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the poll response.
   * @return The client.
   */
  RaftClient poll(String address, PollRequest request, long timeout, Handler<AsyncResult<PollResponse>> resultHandler);

  /**
   * Registers a poll request handler.
   * 
   * @param handler A poll request handler.
   * @return The client.
   */
  RaftClient pollHandler(Handler<PollRequest> handler);

  /**
   * Sends a submit request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param resultHandler An asynchronous handler to be called with the submit response.
   * @return The client.
   */
  RaftClient submit(String address, SubmitRequest request, Handler<AsyncResult<SubmitResponse>> resultHandler);

  /**
   * Sends a submit request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param resultHandler An asynchronous handler to be called with the submit response.
   * @return The client.
   */
  RaftClient submit(String address, SubmitRequest request, long timeout, Handler<AsyncResult<SubmitResponse>> resultHandler);

  /**
   * Registers a submit request handler.
   * 
   * @param handler A submit request handler.
   * @return The client.
   */
  RaftClient submitHandler(Handler<SubmitRequest> handler);

  /**
   * Starts the client.
   * 
   * @return The client.
   */
  RaftClient start();

  /**
   * Starts the client.
   * 
   * @param doneHandler An asynchronous handler to be called once the client is started.
   * @return The replica client.
   */
  RaftClient start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Stops the client.
   */
  void stop();

  /**
   * Stops the client.
   * 
   * @param doneHandler An asynchronous handler to be called once the client is stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

}
