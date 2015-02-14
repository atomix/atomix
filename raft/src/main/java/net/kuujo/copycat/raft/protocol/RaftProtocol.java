/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.raft.protocol;

import net.kuujo.copycat.util.Managed;

import java.util.concurrent.CompletableFuture;

/**
 * Raft protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RaftProtocol extends Managed<Void> {

  /**
   * Sends a protocol join request.
   *
   * @param request The protocol join request.
   * @return A completable future to be completed with the join response.
   */
  CompletableFuture<JoinResponse> join(JoinRequest request);

  /**
   * Registers a protocol join request handler.
   *
   * @param handler A protocol join request handler.
   * @return The Raft protocol.
   */
  RaftProtocol joinHandler(ProtocolHandler<JoinRequest, JoinResponse> handler);

  /**
   * Sends a protocol promote request.
   *
   * @param request The protocol promote request.
   * @return A completable future to be completed with the promote response.
   */
  CompletableFuture<PromoteResponse> promote(PromoteRequest request);

  /**
   * Registers a protocol promote request handler.
   *
   * @param handler A protocol promote request handler.
   * @return The Raft protocol.
   */
  RaftProtocol promoteHandler(ProtocolHandler<PromoteRequest, PromoteResponse> handler);

  /**
   * Sends a protocol leave request.
   *
   * @param request The protocol leave request.
   * @return A completable future to be completed with the leave response.
   */
  CompletableFuture<LeaveResponse> leave(LeaveRequest request);

  /**
   * Registers a protocol leave request handler.
   *
   * @param handler A protocol leave request handler.
   * @return The Raft protocol.
   */
  RaftProtocol leaveHandler(ProtocolHandler<LeaveRequest, LeaveResponse> handler);

  /**
   * Sends a protocol sync request.
   *
   * @param request The protocol sync request.
   * @return A completable future to be completed with the sync response.
   */
  CompletableFuture<SyncResponse> sync(SyncRequest request);

  /**
   * Registers a protocol sync request handler.
   *
   * @param handler A protocol sync request handler.
   * @return The Raft protocol.
   */
  RaftProtocol syncHandler(ProtocolHandler<SyncRequest, SyncResponse> handler);

  /**
   * Sends a protocol poll request.
   *
   * @param request The protocol poll request.
   * @return A completable future to be completed with the poll response.
   */
  CompletableFuture<PollResponse> poll(PollRequest request);

  /**
   * Registers a protocol poll request handler.
   *
   * @param handler A protocol poll request handler.
   * @return The Raft protocol.
   */
  RaftProtocol pollHandler(ProtocolHandler<PollRequest, PollResponse> handler);

  /**
   * Sends a protocol vote request.
   *
   * @param request The protocol vote request.
   * @return A completable future to be completed with the vote response.
   */
  CompletableFuture<VoteResponse> vote(VoteRequest request);

  /**
   * Registers a protocol vote request handler.
   *
   * @param handler A protocol vote request handler.
   * @return The Raft protocol.
   */
  RaftProtocol voteHandler(ProtocolHandler<VoteRequest, VoteResponse> handler);

  /**
   * Sends a protocol append request.
   *
   * @param request The protocol append request.
   * @return A completable future to be completed with the append response.
   */
  CompletableFuture<AppendResponse> append(AppendRequest request);

  /**
   * Registers a protocol append request handler.
   *
   * @param handler A protocol append request handler.
   * @return The Raft protocol.
   */
  RaftProtocol appendHandler(ProtocolHandler<AppendRequest, AppendResponse> handler);

  /**
   * Sends a protocol query request.
   *
   * @param request The protocol query request.
   * @return A completable future to be completed with the query response.
   */
  CompletableFuture<QueryResponse> query(QueryRequest request);

  /**
   * Registers a protocol query request handler.
   *
   * @param handler A protocol query request handler.
   * @return The Raft protocol.
   */
  RaftProtocol queryHandler(ProtocolHandler<QueryRequest, QueryResponse> handler);

  /**
   * Sends a protocol command request.
   *
   * @param request The protocol command request.
   * @return A completable future to be completed with the command response.
   */
  CompletableFuture<CommandResponse> command(CommandRequest request);

  /**
   * Registers a protocol command request handler.
   *
   * @param handler A protocol command request handler.
   * @return The Raft protocol.
   */
  RaftProtocol commandHandler(ProtocolHandler<CommandRequest, CommandResponse> handler);

}
