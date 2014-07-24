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

import net.kuujo.copycat.util.AsyncCallback;

/**
 * Request handler for requests between CopyCat replicas.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ProtocolHandler {

  /**
   * Sends or handles a protocol ping request.
   *
   * @param request The ping request.
   * @param responseCallback A ping response callback.
   */
  void ping(PingRequest request, AsyncCallback<PingResponse> responseCallback);

  /**
   * Sends or handles a protocol sync request.
   *
   * @param request The sync request.
   * @param responseCallback A sync response callback.
   */
  void sync(SyncRequest request, AsyncCallback<SyncResponse> responseCallback);

  /**
   * Sends or handles a protocol install request.
   *
   * @param request The install request.
   * @param responseCallback A install response callback.
   */
  void install(InstallRequest request, AsyncCallback<InstallResponse> responseCallback);

  /**
   * Sends or handles a protocol poll request.
   *
   * @param request The poll request.
   * @param responseCallback A poll response callback.
   */
  void poll(PollRequest request, AsyncCallback<PollResponse> responseCallback);

  /**
   * Sends or handles a protocol submit request.
   *
   * @param request The submit request.
   * @param responseCallback A submit response callback.
   */
  void submit(SubmitRequest request, AsyncCallback<SubmitResponse> responseCallback);

}
