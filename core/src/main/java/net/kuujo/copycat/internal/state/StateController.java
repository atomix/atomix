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
package net.kuujo.copycat.internal.state;

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.event.MembershipChangeEvent;
import net.kuujo.copycat.event.VoteCastEvent;
import net.kuujo.copycat.internal.StateMachineExecutor;
import net.kuujo.copycat.internal.cluster.RemoteNode;
import net.kuujo.copycat.internal.log.ConfigurationEntry;
import net.kuujo.copycat.internal.log.CopycatEntry;
import net.kuujo.copycat.internal.log.OperationEntry;
import net.kuujo.copycat.internal.log.SnapshotEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base replica state controller.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class StateController implements RequestHandler {
  protected StateContext context;
  private final AtomicBoolean transition = new AtomicBoolean();

  /**
   * Returns the controller state.
   *
   * @return The controller state.
   */
  abstract CopycatState state();

  /**
   * Returns the state logger.
   */
  abstract Logger logger();

  /**
   * Logs a request.
   */
  protected final <R extends Request> R logRequest(R request) {
    logger().debug("{} - Received {}", context.clusterManager().localNode(), request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends Response> R logResponse(R response) {
    logger().debug("{} - Sent {}", context.clusterManager().localNode(), response);
    return response;
  }

  /**
   * Initializes the controller.
   *
   * @param context The state context.
   */
  void init(StateContext context) {
    this.context = context;
    context.clusterManager().localNode().server().requestHandler(this);
  }

  @Override
  public synchronized CompletableFuture<PingResponse> ping(final PingRequest request) {
    CompletableFuture<PingResponse> future = CompletableFuture.completedFuture(logResponse(handlePing(logRequest(request))));
    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition.get()) {
      context.transition(FollowerController.class);
    }
    return future;
  }

  /**
   * Handles a ping request.
   */
  private synchronized PingResponse handlePing(PingRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.currentTerm() || (request.term() == context.currentTerm() && context.currentLeader() == null)) {
      context.currentTerm(request.term());
      context.currentLeader(request.leader());
      transition.set(true);
    }

    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.currentTerm()) {
      logger().warn("{} - Rejected {}: request term is less than the current term ({})", context.clusterManager()
        .localNode(), request, context.currentTerm());
      return new PingResponse(request.id(), context.currentTerm(), false);
    } else if (request.logIndex() > 0 && request.logTerm() > 0) {
      return doCheckPingEntry(request);
    }
    return new PingResponse(request.id(), context.currentTerm(), true);
  }

  /**
   * Checks the ping log entry for consistency.
   */
  private synchronized PingResponse doCheckPingEntry(PingRequest request) {
    if (request.logIndex() > context.log().lastIndex()) {
      logger().warn("{} - Rejected {}: previous index ({}) is greater than the local log's last index ({})", context.clusterManager().localNode(), request, request.logIndex(), context.log().lastIndex());
      return new PingResponse(request.id(), context.currentTerm(), false);
    }

    // If the log entry exists then load the entry.
    // If the last log entry's term is not the same as the given
    // prevLogTerm then return false. This will cause the leader to
    // decrement this node's nextIndex and ultimately retry with the
    // leader's previous log entry so that the inconsistent entry
    // can be overwritten.
    CopycatEntry entry = context.log().getEntry(request.logIndex());
    if (entry == null) {
      logger().warn("{} - Rejected {}: request entry not found in local log", context.clusterManager().localNode(), request);
      return new PingResponse(request.id(), context.currentTerm(), false);
    } else if (entry.term() != request.logTerm()) {
      logger().warn("{} - Rejected {}: request entry term does not match local log", context.clusterManager().localNode(), request);
      return new PingResponse(request.id(), context.currentTerm(), false);
    } else {
      doApplyCommits(request.commitIndex());
      return new PingResponse(request.id(), context.currentTerm(), true);
    }
  }

  @Override
  public synchronized CompletableFuture<SyncResponse> sync(final SyncRequest request) {
    CompletableFuture<SyncResponse> future = CompletableFuture.completedFuture(logResponse(handleSync(logRequest(request))));
    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition.get()) {
      context.transition(FollowerController.class);
    }
    return future;
  }

  /**
   * Starts the sync process.
   */
  private synchronized SyncResponse handleSync(SyncRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.currentTerm() || (request.term() == context.currentTerm() && context.currentLeader() == null)) {
      context.currentTerm(request.term());
      context.currentLeader(request.leader());
      transition.set(true);
    }

    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.currentTerm()) {
      logger().warn("{} - Rejected {}: request term is less than the current term ({})", context.clusterManager().localNode(), request, context.currentTerm());
      return new SyncResponse(request.id(), context.currentTerm(), false, context.log().lastIndex());
    } else if (request.prevLogIndex() > 0 && request.prevLogTerm() > 0) {
      return doCheckPreviousEntry(request);
    } else {
      return doAppendEntries(request);
    }
  }

  /**
   * Checks the previous log entry for consistency.
   */
  private synchronized SyncResponse doCheckPreviousEntry(SyncRequest request) {
    if (request.prevLogIndex() > context.log().lastIndex()) {
      logger().warn("{} - Rejected {}: previous index ({}) is greater than the local log's last index ({})", context.clusterManager().localNode(), request, request.prevLogIndex(), context.log().lastIndex());
      return new SyncResponse(request.id(), context.currentTerm(), false, context.log().lastIndex());
    }

    // If the log entry exists then load the entry.
    // If the last log entry's term is not the same as the given
    // prevLogTerm then return false. This will cause the leader to
    // decrement this node's nextIndex and ultimately retry with the
    // leader's previous log entry so that the inconsistent entry
    // can be overwritten.
    CopycatEntry entry = context.log().getEntry(request.prevLogIndex());
    if (entry == null) {
      logger().warn("{} - Rejected {}: request entry not found in local log", context.clusterManager().localNode(), request);
      return new SyncResponse(request.id(), context.currentTerm(), false, context.log().lastIndex());
    } else if (entry.term() != request.prevLogTerm()) {
      logger().warn("{} - Rejected {}: request entry term does not match local log", context.clusterManager().localNode(), request);
      return new SyncResponse(request.id(), context.currentTerm(), false, context.log().lastIndex());
    } else {
      return doAppendEntries(request);
    }
  }

  /**
   * Appends entries to the local log.
   */
  private synchronized SyncResponse doAppendEntries(SyncRequest request) {
    // If the log contains entries after the request's previous log index
    // then remove those entries to be replaced by the request entries.
    if (!request.entries().isEmpty()) {
      synchronized (context.log()) {
        long index = request.prevLogIndex();
        for (CopycatEntry entry : request.<CopycatEntry>entries()) {
          index++;
          // Replicated snapshot entries are *always* immediately logged and applied to the state machine
          // since snapshots are only taken of committed state machine state. This will cause all previous
          // entries to be removed from the log.
          if (entry instanceof SnapshotEntry) {
            installSnapshot(index, (SnapshotEntry) entry);
          } else {
            CopycatEntry match = context.log().getEntry(index);
            if (match != null) {
              if (entry.term() != match.term()) {
                logger().warn("{} - Synced entry does not match local log, removing incorrect entries", context.clusterManager().localNode());
                context.log().removeAfter(index - 1);
                context.log().appendEntry(entry);
                logger().debug("{} - Appended {} to log at index {}", context.clusterManager().localNode(), entry, index);
              }
            } else {
              context.log().appendEntry(entry);
              logger().debug("{} - Appended {} to log at index {}", context.clusterManager().localNode(), entry, index);
            }
          }
        }
      }
    }
    doApplyCommits(request.commitIndex());
    return new SyncResponse(request.id(), context.currentTerm(), true, context.log().lastIndex());
  }

  /**
   * Applies commits to the local state machine.
   */
  private synchronized void doApplyCommits(long commitIndex) {
    // If the synced commit index is greater than the local commit index then
    // apply commits to the local state machine.
    // Also, it's possible that one of the previous command applications failed
    // due to asynchronous communication errors, so alternatively check if the
    // local commit index is greater than last applied. If all the state machine
    // commands have not yet been applied then we want to re-attempt to apply them.
    if (commitIndex > context.commitIndex() || context.commitIndex() > context.lastApplied()) {
      // Update the local commit index with min(request commit, last log // index)
      long lastIndex = context.log().lastIndex();
      context.commitIndex(Math.min(Math.max(commitIndex, context.commitIndex()), lastIndex));

      // If the updated commit index indicates that commits remain to be
      // applied to the state machine, iterate entries and apply them.
      if (context.commitIndex() > context.lastApplied()) {
        // Starting after the last applied entry, iterate through new entries
        // and apply them to the state machine up to the commit index.
        for (long i = context.lastApplied() + 1; i <= Math.min(context.commitIndex(), lastIndex); i++) {
          // Apply the entry to the state machine.
          applyEntry(i);
        }

        // Once entries have been applied check whether we need to compact the log.
        compactLog();
      }
    }
  }

  /**
   * Installs a replicated snapshot.
   */
  private synchronized void installSnapshot(long index, SnapshotEntry entry) {
    logger().info("{} - Installing snapshot {}", context.clusterManager().localNode(), entry);
    logger().info("{} - Compacting log", context.clusterManager().localNode());
    try {
      context.log().compact(index, entry);
    } catch (IOException e) {
      logger().error(e.getMessage());
    }

    applySnapshot(index, (SnapshotEntry) entry);
    context.commitIndex(index);
    context.lastApplied(index);
  }

  /**
   * Applies the entry at the given index.
   *
   * @param index The index of the entry to apply.
   */
  protected void applyEntry(long index) {
    applyEntry(index, context.log().getEntry(index));
  }

  /**
   * Applies the entry at the given index.
   *
   * @param index The index of the entry to apply.
   * @param entry The entry to apply.
   */
  protected void applyEntry(long index, Entry entry) {
    // Validate that the entry being applied is the next entry in the log.
    if (context.lastApplied() == index-1) {
      // Ensure that the entry exists.
      if (entry == null) {
        throw new IllegalStateException("null entry cannot be applied to state machine");
      }
  
      // If the entry is a command entry, apply the command to the state machine.
      if (entry instanceof OperationEntry) {
        applyCommand(index, (OperationEntry) entry);
      }
      // If the entry is a configuration entry, update the local cluster configuration.
      else if (entry instanceof ConfigurationEntry) {
        applyConfig(index, (ConfigurationEntry) entry);
      }
      // If the entry is a snapshot entry, apply the snapshot to the local state machine.
      else if (entry instanceof SnapshotEntry) {
        applySnapshot(index, (SnapshotEntry) entry);
      }
      // If the entry is of another type, e.g. a no-op entry, simply set last applied.
      else {
        context.lastApplied(index);
      }
    }
  }

  /**
   * Applies a command entry to the state machine.
   *
   * @param index The index of the entry being applied.
   * @param entry The entry to apply.
   */
  protected void applyCommand(long index, OperationEntry entry) {
    try {
      logger().debug("{} - Apply operation: {}", context.clusterManager().localNode(), entry.operation());
      StateMachineExecutor.Operation operation = context.stateMachineExecutor().getOperation(entry.operation());
      if (operation != null) {
        operation.apply(entry.args());
      }
    } catch (Exception e) {
    } finally {
      context.lastApplied(index);
    }
  }

  /**
   * Applies a configuration entry to the replica.
   *
   * @param index The index of the entry being applied.
   * @param entry The entry to apply.
   */
  protected void applyConfig(long index, ConfigurationEntry entry) {
    try {
      logger().debug("{} - Apply configuration change: {}", context.clusterManager().localNode(), entry.cluster());
      context.clusterManager().cluster().syncWith(entry.cluster());
      context.events().membershipChange().handle(new MembershipChangeEvent(entry.cluster()));
    } catch (Exception e) {
    } finally {
      context.lastApplied(index);
    }
  }

  /**
   * Applies a single snapshot entry to the state machine.
   *
   * @param index The index of the entry to apply.
   * @param entry The entry to apply.
   */
  protected void applySnapshot(long index, SnapshotEntry entry) {
    logger().debug("{} - Apply snapshot: {}", context.clusterManager().localNode(), entry.cluster());
    synchronized (context.log()) {
      // Apply the snapshot to the local state machine.
      context.stateMachineExecutor().stateMachine().installSnapshot(entry.data());

      // If the log is compactable then compact it at the snapshot index.
      try {
        context.log().compact(index, entry);
      } catch (IOException e) {
        throw new CopycatException(e, "Failed to compact log.");
      }

      // Set the local cluster configuration according to the snapshot cluster membership.
      context.clusterManager().cluster().syncWith(entry.cluster());
      context.events().membershipChange().handle(new MembershipChangeEvent(entry.cluster()));

      // Finally, if necessary, increment the current term.
      context.currentTerm(Math.max(context.currentTerm(), entry.term()));
      context.lastApplied(index);
    }
  }

  /**
   * Creates a snapshot of the state machine state.
   *
   * @return A snapshot of the state machine state.
   */
  protected SnapshotEntry createSnapshot() {
    byte[] snapshot = context.stateMachineExecutor().stateMachine().takeSnapshot();
    if (snapshot != null) {
      return new SnapshotEntry(context.currentTerm(), context.clusterManager().cluster().copy(), snapshot);
    }
    return null;
  }

  /**
   * Compacts the local log.
   */
  protected CompletableFuture<Void> compactLog() {
    // If the log has now grown past the maximum local log size, create a
    // snapshot of the current state machine state and compact the log
    // using the state machine snapshot as the first entry. The snapshot
    // is stored as a log entry in order to simplify replication to
    // out-of-sync followers.
    if (context.log().size() > context.config().getMaxLogSize()) {
      synchronized (context.log()) {
        logger().info("{} - Compacting log", context.clusterManager().localNode());
        final long lastApplied = context.lastApplied();
        final SnapshotEntry snapshot = createSnapshot();
        if (snapshot != null) {
          try {
            context.log().compact(lastApplied, snapshot);
          } catch (IOException e) {
            throw new CopycatException(e, "Failed to compact log.");
          }
        } else {
          logger().warn("{} - Failed to compact log: no snapshot data provided", context.clusterManager().localNode());
        }
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    logger().debug("{} - Received {}", context.clusterManager().localNode(), request);
    return CompletableFuture.completedFuture(logResponse(handlePoll(logRequest(request))));
  }

  /**
   * Handles a vote request.
   */
  private PollResponse handlePoll(PollRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.currentTerm()) {
      context.currentTerm(request.term());
      context.currentLeader(null);
      context.lastVotedFor(null);
    }

    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < context.currentTerm()) {
      logger().debug("{} - Rejected {}: candidate's term is less than the current term", context.clusterManager().localNode(), request);
      return new PollResponse(request.id(), context.currentTerm(), false);
    }
    // If the requesting candidate is ourself then always vote for ourself. Votes
    // for self are done by calling the local node. Note that this obviously
    // doesn't make sense for a leader.
    else if (request.candidate().equals(context.clusterManager().localNode().member().id())) {
      context.lastVotedFor(context.clusterManager().localNode().member().id());
      context.events().voteCast().handle(new VoteCastEvent(context.currentTerm(), context.clusterManager().localNode().member()));
      logger().debug("{} - Accepted {}: candidate is the local node", context.clusterManager().localNode(), request);
      return new PollResponse(request.id(), context.currentTerm(), true);
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (context.clusterManager().node(request.candidate()) == null) {
      logger().debug("{} - Rejected {}: candidate is not known do the local node", context.clusterManager().localNode(), request);
      return new PollResponse(request.id(), context.currentTerm(), false);
    }
    // If we've already voted for someone else then don't vote again.
    else if (context.lastVotedFor() == null || context.lastVotedFor().equals(request.candidate())) {
      // If the log is empty then vote for the candidate.
      if (context.log().isEmpty()) {
        context.lastVotedFor(request.candidate());
        context.events().voteCast().handle(new VoteCastEvent(context.currentTerm(), context.clusterManager().node(request.candidate()).member()));
        logger().debug("{} - Accepted {}: candidate's log is up-to-date", context.clusterManager().localNode(), request);
        return new PollResponse(request.id(), context.currentTerm(), true);
      } else {
        // Otherwise, load the last entry in the log. The last entry should be
        // at least as up to date as the candidates entry and term.
        synchronized (context.log()) {
          long lastIndex = context.log().lastIndex();
          CopycatEntry entry = context.log().getEntry(lastIndex);
          if (entry == null) {
            context.lastVotedFor(request.candidate());
            context.events()
              .voteCast()
              .handle(new VoteCastEvent(context.currentTerm(), context.clusterManager()
                .node(request.candidate())
                .member()));
            logger().debug("{} - Accepted {}: candidate's log is up-to-date", context.clusterManager().localNode(), request);
            return new PollResponse(request.id(), context.currentTerm(), true);
          }

          long lastTerm = entry.term();
          if (request.lastLogIndex() >= lastIndex) {
            if (request.lastLogTerm() >= lastTerm) {
              context.lastVotedFor(request.candidate());
              context.events()
                .voteCast()
                .handle(new VoteCastEvent(context.currentTerm(), context.clusterManager()
                  .node(request.candidate())
                  .member()));
              logger().debug("{} - Accepted {}: candidate's log is up-to-date", context.clusterManager().localNode(), request);
              return new PollResponse(request.id(), context.currentTerm(), true);
            } else {
              logger().debug("{} - Rejected {}: candidate's last log term ({}) is in conflict with local log ({})", context.clusterManager().localNode(), request, request.lastLogTerm(), lastTerm);
              return new PollResponse(request.id(), context.currentTerm(), false);
            }
          } else {
            logger().debug("{} - Rejected {}: candidate's last log entry ({}) is at a lower index than the local log ({})", context.clusterManager().localNode(), request, request.lastLogIndex(), lastIndex);
            return new PollResponse(request.id(), context.currentTerm(), false);
          }
        }
      }
    }
    // In this case, we've already voted for someone else.
    else {
      logger().debug("{} - Rejected {}: already voted for {}", context.clusterManager().localNode(), request, context.lastVotedFor());
      return new PollResponse(request.id(), context.currentTerm(), false);
    }
  }

  @Override
  public CompletableFuture<SubmitResponse> submit(SubmitRequest request) {
    logRequest(request);

    // If consistent queries are disabled then this node can accept reads regardless of the
    // current state of the system. Note that this only applies to read-only operations.
    CompletableFuture<SubmitResponse> future = new CompletableFuture<>();
    if (!context.config().isConsistentQueryExecution()) {
      StateMachineExecutor.Operation operation = context.stateMachineExecutor().getOperation(request.operation());
      if (operation != null && operation.isReadOnly()) {
        try {
          future.complete(logResponse(new SubmitResponse(request.id(), operation.apply(request.args()))));
        } catch (Exception e) {
          future.completeExceptionally(e);
        }
        return future;
      }
    }

    // If the cluster has a leader then locate the leader and forward the operation.
    if (context.currentLeader() != null) {
      RemoteNode leader = context.clusterManager().remoteNode(context.currentLeader());
      if (leader != null) {
        logger().debug("{} - Forwarding {} to leader {}", context.clusterManager().localNode(), request, leader);
        leader.client().connect().whenComplete((r, e) -> {
          if (e != null) {
            future.completeExceptionally(e);
          } else {
            leader.client().submit(request).whenComplete((result, error) -> {
              if (error != null) {
                future.completeExceptionally(error);
              } else {
                future.complete(result);
              }
            });
          }
        });
        return future;
      }
    }
    return CompletableFuture.completedFuture(logResponse(new SubmitResponse(request.id(), "Not the leader")));
  }

  /**
   * Destroys the state controller.
   */
  void destroy() {
    context.clusterManager().localNode().server().requestHandler(null);
  }

  @Override
  public String toString() {
    return String.format("%s[context=%s]", getClass().getSimpleName(), context);
  }

}
