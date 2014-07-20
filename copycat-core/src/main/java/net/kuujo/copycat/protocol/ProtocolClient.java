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
 * Protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ProtocolClient {

  /**
   * Initializes the client.
   *
   * @param context The copycat context.
   */
  void init(CopyCatContext context);

  /**
   * Sends a ping request.
   *
   * @param request The ping request.
   * @param callback A callback to be called once the response is received.
   */
  void ping(PingRequest request, AsyncCallback<PingResponse> callback);

  /**
   * Sends a sync request.
   *
   * @param request The sync request.
   * @param callback A callback to be called once the response is received.
   */
  void sync(SyncRequest request, AsyncCallback<SyncResponse> callback);

  /**
   * Sends an install request.
   *
   * @param request The install request.
   * @param callback A callback to be called once the response is received.
   */
  void install(InstallRequest request, AsyncCallback<InstallResponse> callback);

  /**
   * Sends a poll request.
   *
   * @param request The poll request.
   * @param callback A callback to be called once the response is received.
   */
  void poll(PollRequest request, AsyncCallback<PollResponse> callback);

  /**
   * Sends a submit request.
   *
   * @param request The submit request.
   * @param callback A callback to be called once the response is received.
   */
  void submit(SubmitRequest request, AsyncCallback<SubmitResponse> callback);

}
