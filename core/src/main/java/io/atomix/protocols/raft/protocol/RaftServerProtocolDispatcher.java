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

import io.atomix.cluster.NodeId;

import java.util.concurrent.CompletableFuture;

/**
 * Raft server protocol dispatcher.
 */
public interface RaftServerProtocolDispatcher {

  /**
   * Sends an open session request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<OpenSessionResponse> openSession(NodeId nodeId, OpenSessionRequest request);

  /**
   * Sends a close session request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<CloseSessionResponse> closeSession(NodeId nodeId, CloseSessionRequest request);

  /**
   * Sends a keep alive request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<KeepAliveResponse> keepAlive(NodeId nodeId, KeepAliveRequest request);

  /**
   * Sends a query request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<QueryResponse> query(NodeId nodeId, QueryRequest request);

  /**
   * Sends a command request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<CommandResponse> command(NodeId nodeId, CommandRequest request);

  /**
   * Sends a metadata request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<MetadataResponse> metadata(NodeId nodeId, MetadataRequest request);

  /**
   * Sends a join request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<JoinResponse> join(NodeId nodeId, JoinRequest request);

  /**
   * Sends a leave request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<LeaveResponse> leave(NodeId nodeId, LeaveRequest request);

  /**
   * Sends a configure request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<ConfigureResponse> configure(NodeId nodeId, ConfigureRequest request);

  /**
   * Sends a reconfigure request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<ReconfigureResponse> reconfigure(NodeId nodeId, ReconfigureRequest request);

  /**
   * Sends an install request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<InstallResponse> install(NodeId nodeId, InstallRequest request);

  /**
   * Sends a poll request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<PollResponse> poll(NodeId nodeId, PollRequest request);

  /**
   * Sends a vote request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<VoteResponse> vote(NodeId nodeId, VoteRequest request);

  /**
   * Sends an append request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<AppendResponse> append(NodeId nodeId, AppendRequest request);

  /**
   * Unicasts a publish request to the given node.
   *
   * @param nodeId  the node to which to send the request
   * @param request the request to send
   */
  void publish(NodeId nodeId, PublishRequest request);

}
