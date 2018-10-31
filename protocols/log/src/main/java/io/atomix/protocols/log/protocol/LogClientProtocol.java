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
package io.atomix.protocols.log.protocol;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import io.atomix.cluster.MemberId;

/**
 * Primary-backup protocol client.
 */
public interface LogClientProtocol {

  /**
   * Sends an append request to the given node.
   *
   * @param memberId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<AppendResponse> append(MemberId memberId, AppendRequest request);

  /**
   * Sends a consume request to the given node.
   *
   * @param memberId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<ConsumeResponse> consume(MemberId memberId, ConsumeRequest request);

  /**
   * Sends a reset request to the given node.
   *
   * @param memberId  the node to which to send the request
   * @param request the request to send
   */
  void reset(MemberId memberId, ResetRequest request);

  /**
   * Registers a records request callback.
   *
   * @param subject the records request subject
   * @param handler the records request handler to register
   * @param executor the records request executor
   */
  void registerRecordsConsumer(String subject, Consumer<RecordsRequest> handler, Executor executor);

  /**
   * Unregisters the append request handler.
   *
   * @param subject the records request subject
   */
  void unregisterRecordsConsumer(String subject);

}
