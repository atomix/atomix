/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.backup.protocol;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Primary-backup protocol client.
 */
public interface PrimaryBackupClientProtocol {

  /**
   * Sends an execute request to the given node.
   *
   * @param memberId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<ExecuteResponse> execute(MemberId memberId, ExecuteRequest request);

  /**
   * Sends a metadata request to the given node.
   *
   * @param memberId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<MetadataResponse> metadata(MemberId memberId, MetadataRequest request);

  /**
   * Sends a close request to the given node.
   *
   * @param memberId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<CloseResponse> close(MemberId memberId, CloseRequest request);

  /**
   * Registers a publish request listener.
   *
   * @param sessionId the session for which to listen for the publish request
   * @param listener  the listener to register
   * @param executor  the executor with which to execute the listener callback
   */
  void registerEventListener(SessionId sessionId, Consumer<PrimitiveEvent> listener, Executor executor);

  /**
   * Unregisters the publish request listener for the given session.
   *
   * @param sessionId the session for which to unregister the listener
   */
  void unregisterEventListener(SessionId sessionId);

}
