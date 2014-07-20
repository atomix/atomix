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
 * Protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ProtocolServer {

  /**
   * Registers a ping request callback.
   *
   * @param callback A callback to be called when a ping request is received.
   */
  void pingCallback(AsyncCallback<PingRequest> callback);

  /**
   * Registers a sync request callback.
   *
   * @param callback A callback to be called when a sync request is received.
   */
  void syncCallback(AsyncCallback<SyncRequest> callback);

  /**
   * Registers an install request callback.
   *
   * @param callback A callback to be called when an install request is received.
   */
  void installCallback(AsyncCallback<InstallRequest> callback);

  /**
   * Registers a poll request callback.
   *
   * @param callback A callback to be called when a poll request is received.
   */
  void pollCallback(AsyncCallback<PollRequest> callback);

  /**
   * Registers a submit request callback.
   *
   * @param callback A callback to be called when a submit request is received.
   */
  void submitCallback(AsyncCallback<SubmitRequest> callback);

  /**
   * Starts the server.
   *
   * @param callback A callback to be called once complete.
   */
  void start(AsyncCallback<Void> callback);

  /**
   * Starts the server.
   *
   * @param callback A callback to be called once complete.
   */
  void stop(AsyncCallback<Void> callback);

}
