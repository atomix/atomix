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
import java.util.function.Function;

import io.atomix.cluster.MemberId;

/**
 * Primary-backup server protocol.
 */
public interface LogServerProtocol {

  /**
   * Sends a records request to the given node.
   *
   * @param memberId the node to which to send the request
   * @param subject the subject to which to send the request
   * @param request the request to send
   */
  void produce(MemberId memberId, String subject, RecordsRequest request);

  /**
   * Sends a backup request to the given node.
   *
   * @param memberId the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<BackupResponse> backup(MemberId memberId, BackupRequest request);

  /**
   * Registers an append request callback.
   *
   * @param handler the append request handler to register
   */
  void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler);

  /**
   * Unregisters the append request handler.
   */
  void unregisterAppendHandler();

  /**
   * Registers a consume request callback.
   *
   * @param handler the read request handler to register
   */
  void registerConsumeHandler(Function<ConsumeRequest, CompletableFuture<ConsumeResponse>> handler);

  /**
   * Unregisters the consume request handler.
   */
  void unregisterConsumeHandler();

  /**
   * Registers a reset consumer.
   *
   * @param consumer the consumer to register
   * @param executor the consumer executor
   */
  void registerResetConsumer(Consumer<ResetRequest> consumer, Executor executor);

  /**
   * Unregisters the reset request handler.
   */
  void unregisterResetConsumer();

  /**
   * Registers a backup request callback.
   *
   * @param handler the backup request handler to register
   */
  void registerBackupHandler(Function<BackupRequest, CompletableFuture<BackupResponse>> handler);

  /**
   * Unregisters the backup request handler.
   */
  void unregisterBackupHandler();

}
