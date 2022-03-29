// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
