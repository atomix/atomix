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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.util.AsyncCallback;

/**
 * CopyCat protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Protocol {

  /**
   * Initializes the protocol.
   *
   * @param context The copycat context.
   */
  void init(CopyCatContext context);

  /**
   * Sends a ping request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param callback An asynchronous callback to be called with the ping response.
   * @return The protocol instance.
   */
  Protocol ping(String address, PingRequest request, long timeout, AsyncCallback<PingResponse> callback);

  /**
   * Registers a ping request callback.
   * 
   * @param callback A ping request callback.
   * @return The protocol instance.
   */
  Protocol pingCallback(AsyncCallback<PingRequest> callback);

  /**
   * Sends a sync request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param callback An asynchronous callback to be called with the sync response.
   * @return The protocol instance.
   */
  Protocol sync(String address, SyncRequest request, long timeout, AsyncCallback<SyncResponse> callback);

  /**
   * Registers async request callback.
   * 
   * @param callback An append entries request callback.
   * @return The protocol instance.
   */
  Protocol syncCallback(AsyncCallback<SyncRequest> callback);

  /**
   * Sends an install request to a node.
   *
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param callback An asynchronous callback to be called with the poll response.
   * @return The poll response.
   */
  Protocol install(String address, InstallRequest request, long timeout, AsyncCallback<InstallResponse> callback);

  /**
   * Registers an install request callback.
   *
   * @param callback An install request callback.
   * @return The protocol instance.
   */
  Protocol installCallback(AsyncCallback<InstallRequest> callback);

  /**
   * Sends a poll request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param callback An asynchronous callback to be called with the poll response.
   * @return The protocol instance.
   */
  Protocol poll(String address, PollRequest request, long timeout, AsyncCallback<PollResponse> callback);

  /**
   * Registers a poll request callback.
   * 
   * @param callback A poll request callback.
   * @return The protocol instance.
   */
  Protocol pollCallback(AsyncCallback<PollRequest> callback);

  /**
   * Sends a submit request to a service.
   * 
   * @param address The address to which to send the request.
   * @param request The request to send.
   * @param timeout The request/response timeout.
   * @param callback An asynchronous callback to be called with the submit response.
   * @return The protocol instance.
   */
  Protocol submit(String address, SubmitRequest request, long timeout, AsyncCallback<SubmitResponse> callback);

  /**
   * Registers a submit request callback.
   * 
   * @param callback A submit request callback.
   * @return The protocol instance.
   */
  Protocol submitCallback(AsyncCallback<SubmitRequest> callback);

  /**
   * Starts the client.
   * 
   * @param callback An asynchronous callback to be called once the client is started.
   * @return The protocol instance.
   */
  Protocol start(AsyncCallback<Void> callback);

  /**
   * Stops the client.
   * 
   * @param callback An asynchronous callback to be called once the client is stopped.
   */
  void stop(AsyncCallback<Void> callback);

}
