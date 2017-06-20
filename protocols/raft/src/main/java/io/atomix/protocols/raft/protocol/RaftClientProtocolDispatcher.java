/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.cluster.MemberId;

import java.util.concurrent.CompletableFuture;

/**
 * Raft client protocol dispatcher.
 */
public interface RaftClientProtocolDispatcher {

  /**
   * Sends an open session request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<OpenSessionResponse> openSession(MemberId nodeId, OpenSessionRequest request);

  /**
   * Sends a close session request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<CloseSessionResponse> closeSession(MemberId nodeId, CloseSessionRequest request);

  /**
   * Sends a keep alive request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<KeepAliveResponse> keepAlive(MemberId nodeId, KeepAliveRequest request);

  /**
   * Sends a query request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<QueryResponse> query(MemberId nodeId, QueryRequest request);

  /**
   * Sends a command request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<CommandResponse> command(MemberId nodeId, CommandRequest request);

  /**
   * Sends a metadata request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<MetadataResponse> metadata(MemberId nodeId, MetadataRequest request);

  /**
   * Broadcasts a reset request to all nodes in the cluster.
   *
   * @param request the reset request to broadcast
   */
  void reset(ResetRequest request);

}
