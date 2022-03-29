// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.roles;

import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.CloseSessionResponse;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.ConfigureRequest;
import io.atomix.protocols.raft.protocol.ConfigureResponse;
import io.atomix.protocols.raft.protocol.InstallRequest;
import io.atomix.protocols.raft.protocol.InstallResponse;
import io.atomix.protocols.raft.protocol.JoinRequest;
import io.atomix.protocols.raft.protocol.JoinResponse;
import io.atomix.protocols.raft.protocol.KeepAliveRequest;
import io.atomix.protocols.raft.protocol.KeepAliveResponse;
import io.atomix.protocols.raft.protocol.LeaveRequest;
import io.atomix.protocols.raft.protocol.LeaveResponse;
import io.atomix.protocols.raft.protocol.MetadataRequest;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.OpenSessionRequest;
import io.atomix.protocols.raft.protocol.OpenSessionResponse;
import io.atomix.protocols.raft.protocol.PollRequest;
import io.atomix.protocols.raft.protocol.PollResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.ReconfigureRequest;
import io.atomix.protocols.raft.protocol.ReconfigureResponse;
import io.atomix.protocols.raft.protocol.TransferRequest;
import io.atomix.protocols.raft.protocol.TransferResponse;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.utils.Managed;

import java.util.concurrent.CompletableFuture;

/**
 * Raft role interface.
 */
public interface RaftRole extends Managed<RaftRole> {

  /**
   * Returns the server state type.
   *
   * @return The server state type.
   */
  RaftServer.Role role();

  /**
   * Handles a metadata request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<MetadataResponse> onMetadata(MetadataRequest request);

  /**
   * Handles an open session request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<OpenSessionResponse> onOpenSession(OpenSessionRequest request);

  /**
   * Handles a keep alive request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<KeepAliveResponse> onKeepAlive(KeepAliveRequest request);

  /**
   * Handles a close session request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<CloseSessionResponse> onCloseSession(CloseSessionRequest request);

  /**
   * Handles a configure request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<ConfigureResponse> onConfigure(ConfigureRequest request);

  /**
   * Handles an install request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<InstallResponse> onInstall(InstallRequest request);

  /**
   * Handles a join request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<JoinResponse> onJoin(JoinRequest request);

  /**
   * Handles a configure request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<ReconfigureResponse> onReconfigure(ReconfigureRequest request);

  /**
   * Handles a leave request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<LeaveResponse> onLeave(LeaveRequest request);

  /**
   * Handles a transfer request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<TransferResponse> onTransfer(TransferRequest request);

  /**
   * Handles an append request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<AppendResponse> onAppend(AppendRequest request);

  /**
   * Handles a poll request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<PollResponse> onPoll(PollRequest request);

  /**
   * Handles a vote request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<VoteResponse> onVote(VoteRequest request);

  /**
   * Handles a command request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<CommandResponse> onCommand(CommandRequest request);

  /**
   * Handles a query request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<QueryResponse> onQuery(QueryRequest request);

}
