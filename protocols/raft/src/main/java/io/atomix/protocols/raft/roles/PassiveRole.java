/*
 * Copyright 2015-present Open Networking Laboratory
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
package io.atomix.protocols.raft.roles;

import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.error.RaftError;
import io.atomix.protocols.raft.error.RaftException;
import io.atomix.protocols.raft.impl.OperationResult;
import io.atomix.protocols.raft.impl.RaftServerContext;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.InstallRequest;
import io.atomix.protocols.raft.protocol.InstallResponse;
import io.atomix.protocols.raft.protocol.OperationResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.session.impl.RaftSessionContext;
import io.atomix.protocols.raft.storage.log.RaftLogWriter;
import io.atomix.protocols.raft.storage.log.entry.QueryEntry;
import io.atomix.protocols.raft.storage.log.entry.RaftLogEntry;
import io.atomix.protocols.raft.storage.snapshot.Snapshot;
import io.atomix.protocols.raft.storage.snapshot.SnapshotWriter;
import io.atomix.protocols.raft.storage.snapshot.StateMachineId;
import io.atomix.storage.journal.Indexed;
import io.atomix.time.WallClockTimestamp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Passive state.
 */
public class PassiveRole extends ReserveRole {
  private final Map<Long, Snapshot> pendingSnapshots = new HashMap<>();
  private int nextSnapshotOffset;

  public PassiveRole(RaftServerContext context) {
    super(context);
  }

  @Override
  public RaftServer.Role role() {
    return RaftServer.Role.PASSIVE;
  }

  @Override
  public CompletableFuture<RaftRole> open() {
    return super.open()
        .thenRun(this::truncateUncommittedEntries)
        .thenApply(v -> this);
  }

  /**
   * Truncates uncommitted entries from the log.
   */
  private void truncateUncommittedEntries() {
    if (role() == RaftServer.Role.PASSIVE) {
      final RaftLogWriter writer = context.getLogWriter();
      writer.getLock().lock();
      try {
        writer.truncate(context.getCommitIndex());
      } finally {
        writer.getLock().unlock();
      }
    }
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(final AppendRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());
    return handleAppend(request);
  }

  /**
   * Handles an AppendRequest.
   */
  protected CompletableFuture<AppendResponse> handleAppend(final AppendRequest request) {
    CompletableFuture<AppendResponse> future = new CompletableFuture<>();

    final RaftLogWriter writer = context.getLogWriter();
    writer.getLock().lock();
    try {
      // Check that the term of the given request matches the local term or update the term.
      if (!checkTerm(request, writer, future)) {
        return future;
      }

      // Check that the previous index/term matches the local log's last entry.
      if (!checkPreviousEntry(request, writer, future)) {
        return future;
      }

      // Append the entries to the log.
      appendEntries(request, writer, future);
    } finally {
      writer.getLock().unlock();
    }
    return future;
  }

  /**
   * Checks the leader's term of the given AppendRequest, returning a boolean indicating whether to continue
   * handling the request.
   */
  protected boolean checkTerm(AppendRequest request, RaftLogWriter writer, CompletableFuture<AppendResponse> future) {
    if (request.term() < context.getTerm()) {
      LOGGER.debug("{} - Rejected {}: request term is less than the current term ({})", context.getCluster().getMember().memberId(), request, context.getTerm());
      return failAppend(writer.getLastIndex(), future);
    }
    return true;
  }

  /**
   * Checks the previous index of the given AppendRequest, returning a boolean indicating whether to continue
   * handling the request.
   */
  protected boolean checkPreviousEntry(AppendRequest request, RaftLogWriter writer, CompletableFuture<AppendResponse> future) {
    // Get the last entry written to the log.
    Indexed<RaftLogEntry> lastEntry = writer.getLastEntry();

    // If the local log is non-empty...
    if (lastEntry != null) {
      // If the previous log index is greater than the last entry index, fail the attempt.
      if (request.prevLogIndex() > lastEntry.index()) {
        LOGGER.debug("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getCluster().getMember().memberId(), request, request.prevLogIndex(), lastEntry.index());
        return failAppend(0, future);
      }

      // If the previous log index is less than the last entry index, fail the attempt.
      if (request.prevLogIndex() < lastEntry.index()) {
        LOGGER.debug("{} - Rejected {}: Previous index ({}) is less than the local log's last index ({})", context.getCluster().getMember().memberId(), request, request.prevLogIndex(), lastEntry.index());
        return failAppend(lastEntry.index(), future);
      }

      // If the previous log term doesn't equal the last entry term, fail the append, sending the prior entry.
      if (request.prevLogTerm() != lastEntry.entry().term()) {
        LOGGER.debug("{} - Rejected {}: Previous entry term ({}) does not equal the local log's last term ({})", context.getCluster().getMember().memberId(), request, request.prevLogTerm(), lastEntry.entry().term());
        return failAppend(lastEntry.index() - 1, future);
      }
    } else {
      // If the previous log index is set and the last entry is null, fail the append.
      if (request.prevLogIndex() > 0) {
        LOGGER.debug("{} - Rejected {}: Previous index ({}) is greater than the local log's last index (0)", context.getCluster().getMember().memberId(), request, request.prevLogIndex());
        return failAppend(0, future);
      }
    }
    return true;
  }

  /**
   * Appends entries from the given AppendRequest.
   */
  protected void appendEntries(AppendRequest request, RaftLogWriter writer, CompletableFuture<AppendResponse> future) {
    // Compute the last entry index from the previous log index and request entry count.
    final long lastEntryIndex = request.prevLogIndex() + request.entries().size();

    // Ensure the commitIndex is not increased beyond the index of the last entry in the request.
    final long commitIndex = Math.max(context.getCommitIndex(), Math.min(request.commitIndex(), lastEntryIndex));

    // Track the last log index while entries are appended.
    long lastLogIndex = request.prevLogIndex();

    // Iterate through entries and append them.
    for (RaftLogEntry entry : request.entries()) {
      // If the entry index is greater than the commitIndex, break the loop.
      writer.append(entry);
      LOGGER.trace("{} - Appended {}", context.getCluster().getMember().memberId(), entry);

      // If the last log index meets the commitIndex, break the append loop to avoid appending uncommitted entries.
      if (++lastLogIndex == commitIndex) {
        break;
      }
    }

    // Update the context commit and global indices.
    long previousCommitIndex = context.getCommitIndex();
    context.setCommitIndex(commitIndex);

    if (context.getCommitIndex() > previousCommitIndex) {
      LOGGER.trace("{} - Committed entries up to index {}", context.getCluster().getMember().memberId(), commitIndex);
    }

    // Apply commits to the state machine in batch.
    context.getStateMachine().applyAll(context.getCommitIndex());

    // Return a successful append response.
    succeedAppend(lastLogIndex, future);
  }

  /**
   * Returns a failed append response.
   *
   * @param lastLogIndex the last log index
   * @param future the append response future
   * @return the append response status
   */
  protected boolean failAppend(long lastLogIndex, CompletableFuture<AppendResponse> future) {
    return completeAppend(false, lastLogIndex, future);
  }

  /**
   * Returns a successful append response.
   *
   * @param lastLogIndex the last log index
   * @param future the append response future
   * @return the append response status
   */
  protected boolean succeedAppend(long lastLogIndex, CompletableFuture<AppendResponse> future) {
    return completeAppend(true, lastLogIndex, future);
  }

  /**
   * Returns a successful append response.
   *
   * @param succeeded whether the append succeeded
   * @param lastLogIndex the last log index
   * @param future the append response future
   * @return the append response status
   */
  protected boolean completeAppend(boolean succeeded, long lastLogIndex, CompletableFuture<AppendResponse> future) {
    future.complete(logResponse(AppendResponse.newBuilder()
        .withStatus(RaftResponse.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(succeeded)
        .withLastLogIndex(lastLogIndex)
        .build()));
    return succeeded;
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(QueryRequest request) {
    context.checkThread();
    logRequest(request);

    // If this server has not yet applied entries up to the client's session ID, forward the
    // query to the leader. This ensures that a follower does not tell the client its session
    // doesn't exist if the follower hasn't had a chance to see the session's registration entry.
    if (context.getStateMachine().getLastApplied() < request.session()) {
      LOGGER.trace("{} - State out of sync, forwarding query to leader", context.getCluster().getMember().memberId());
      return queryForward(request);
    }

    // Look up the client's session.
    RaftSessionContext session = context.getStateMachine().getSessions().getSession(request.session());
    if (session == null) {
      LOGGER.trace("{} - State out of sync, forwarding query to leader", context.getCluster().getMember().memberId());
      return queryForward(request);
    }

    // If the session's consistency level is SEQUENTIAL, handle the request here, otherwise forward it.
    if (session.readConsistency() == ReadConsistency.SEQUENTIAL) {

      // If the commit index is not in the log then we've fallen too far behind the leader to perform a local query.
      // Forward the request to the leader.
      if (context.getLogWriter().getLastIndex() < context.getCommitIndex()) {
        LOGGER.trace("{} - State out of sync, forwarding query to leader", context.getCluster().getMember().memberId());
        return queryForward(request);
      }

      final Indexed<QueryEntry> entry = new Indexed<>(
          request.index(),
          new QueryEntry(
              context.getTerm(),
              System.currentTimeMillis(),
              request.session(),
              request.sequenceNumber(),
              request.operation()), 0);

      return applyQuery(entry).thenApply(this::logResponse);
    } else {
      return queryForward(request);
    }
  }

  /**
   * Forwards the query to the leader.
   */
  private CompletableFuture<QueryResponse> queryForward(QueryRequest request) {
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
    }

    LOGGER.trace("{} - Forwarding {}", context.getCluster().getMember().memberId(), request);
    return forward(request, context.getProtocol()::query)
        .exceptionally(error -> QueryResponse.newBuilder()
            .withStatus(RaftResponse.Status.ERROR)
            .withError(RaftError.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
  }

  /**
   * Performs a local query.
   */
  protected CompletableFuture<QueryResponse> queryLocal(Indexed<QueryEntry> entry) {
    return applyQuery(entry);
  }

  /**
   * Applies a query to the state machine.
   */
  protected CompletableFuture<QueryResponse> applyQuery(Indexed<QueryEntry> entry) {
    // In the case of the leader, the state machine is always up to date, so no queries will be queued and all query
    // indexes will be the last applied index.
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    context.getStateMachine().<OperationResult>apply(entry).whenComplete((result, error) -> {
      completeOperation(result, QueryResponse.newBuilder(), error, future);
    });
    return future;
  }

  /**
   * Completes an operation.
   */
  protected <T extends OperationResponse> void completeOperation(OperationResult result, OperationResponse.Builder<?, T> builder, Throwable error, CompletableFuture<T> future) {
    if (isOpen()) {
      if (result != null) {
        builder.withIndex(result.index());
        builder.withEventIndex(result.eventIndex());
        if (result.failed()) {
          error = result.error();
        }
      }

      if (error == null) {
        future.complete(builder.withStatus(RaftResponse.Status.OK)
            .withResult(result != null ? result.result() : null)
            .build());
      } else if (error instanceof CompletionException && error.getCause() instanceof RaftException) {
        future.complete(builder.withStatus(RaftResponse.Status.ERROR)
            .withError(((RaftException) error.getCause()).getType())
            .build());
      } else if (error instanceof RaftException) {
        future.complete(builder.withStatus(RaftResponse.Status.ERROR)
            .withError(((RaftException) error).getType())
            .build());
      } else {
        LOGGER.warn("An unexpected error occurred: {}", error);
        future.complete(builder.withStatus(RaftResponse.Status.ERROR)
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build());
      }
    }
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(InstallRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    // If the request is for a lesser term, reject the request.
    if (request.term() < context.getTerm()) {
      return CompletableFuture.completedFuture(logResponse(InstallResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
          .build()));
    }

    // Get the pending snapshot for the associated snapshot ID.
    Snapshot pendingSnapshot = pendingSnapshots.get(request.snapshotId());

    // If a snapshot is currently being received and the snapshot versions don't match, simply
    // close the existing snapshot. This is a naive implementation that assumes that the leader
    // will be responsible in sending the correct snapshot to this server. Leaders must dictate
    // where snapshots must be sent since entries can still legitimately exist prior to the snapshot,
    // and so snapshots aren't simply sent at the beginning of the follower's log, but rather the
    // leader dictates when a snapshot needs to be sent.
    if (pendingSnapshot != null && request.snapshotIndex() != pendingSnapshot.index()) {
      pendingSnapshot.close();
      pendingSnapshot.delete();
      pendingSnapshot = null;
      nextSnapshotOffset = 0;
    }

    // If there is no pending snapshot, create a new snapshot.
    if (pendingSnapshot == null) {
      // For new snapshots, the initial snapshot offset must be 0.
      if (request.chunkOffset() > 0) {
        return CompletableFuture.completedFuture(logResponse(InstallResponse.newBuilder()
            .withStatus(RaftResponse.Status.ERROR)
            .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
            .build()));
      }

      pendingSnapshot = context.getSnapshotStore().newSnapshot(
              StateMachineId.from(request.snapshotId()),
              request.snapshotIndex(),
              WallClockTimestamp.from(request.snapshotTimestamp()));
      nextSnapshotOffset = 0;
    }

    // If the request offset is greater than the next expected snapshot offset, fail the request.
    if (request.chunkOffset() > nextSnapshotOffset) {
      return CompletableFuture.completedFuture(logResponse(InstallResponse.newBuilder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
          .build()));
    }

    // Write the data to the snapshot.
    try (SnapshotWriter writer = pendingSnapshot.openWriter()) {
      writer.write(request.data());
    }

    // If the snapshot is complete, store the snapshot and reset state, otherwise update the next snapshot offset.
    if (request.complete()) {
      pendingSnapshot.persist().complete();
      pendingSnapshots.remove(request.snapshotId());
      nextSnapshotOffset = 0;
    } else {
      nextSnapshotOffset++;
      pendingSnapshots.put(request.snapshotId(), pendingSnapshot);
    }

    return CompletableFuture.completedFuture(logResponse(InstallResponse.newBuilder()
        .withStatus(RaftResponse.Status.OK)
        .build()));
  }

  @Override
  public CompletableFuture<Void> close() {
    for (Snapshot pendingSnapshot : pendingSnapshots.values()) {
      pendingSnapshot.close();
      pendingSnapshot.delete();
    }
    return super.close();
  }

}
