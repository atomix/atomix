// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
