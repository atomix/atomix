/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.state.impl;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import net.kuujo.copycat.CopyCatException;
import net.kuujo.copycat.event.VoteCastEvent;
import net.kuujo.copycat.log.Compactable;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.impl.CommandEntry;
import net.kuujo.copycat.log.impl.ConfigurationEntry;
import net.kuujo.copycat.log.impl.RaftEntry;
import net.kuujo.copycat.log.impl.SnapshotEntry;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.state.State;

/**
 * Base replica state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class RaftState implements State<RaftStateContext> {
  protected RaftStateContext context;
  private final Executor executor = Executors.newSingleThreadExecutor();
  private final AtomicBoolean transition = new AtomicBoolean();

  @Override
  public void init(RaftStateContext context) {
    this.context = context;
    context.cluster().localMember().protocol().server().protocolHandler(this);
  }

  @Override
  public CompletableFuture<AppendEntriesResponse> appendEntries(final AppendEntriesRequest request) {
    CompletableFuture<AppendEntriesResponse> future = CompletableFuture.completedFuture(handleAppendEntries(request));
    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition.get()) {
      context.transition(Follower.class);
    }
    return future;
  }

  /**
   * Starts the append entries process.
   */
  private AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getCurrentTerm() || (request.term() == context.getCurrentTerm() && context.getCurrentLeader() == null)) {
      context.setCurrentTerm(request.term());
      context.setCurrentLeader(request.leader());
      transition.set(true);
    }

    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getCurrentTerm()) {
      return new AppendEntriesResponse(request.id(), context.getCurrentTerm(), false, context.log().lastIndex());
    } else if (request.prevLogIndex() > 0 && request.prevLogTerm() > 0) {
      return doCheckPreviousEntry(request);
    } else {
      return doAppendEntries(request);
    }
  }

  /**
   * Checks the previous log entry for consistency.
   */
  private AppendEntriesResponse doCheckPreviousEntry(AppendEntriesRequest request) {
    if (request.prevLogIndex() > context.log().lastIndex()) {
      return new AppendEntriesResponse(request.id(), context.getCurrentTerm(), false, context.log().lastIndex());
    }

    // If the log entry exists then load the entry.
    // If the last log entry's term is not the same as the given
    // prevLogTerm then return false. This will cause the leader to
    // decrement this node's nextIndex and ultimately retry with the
    // leader's previous log entry so that the inconsistent entry
    // can be overwritten.
    RaftEntry entry = context.log().getEntry(request.prevLogIndex());
    if (entry == null || entry.term() != request.prevLogTerm()) {
      return new AppendEntriesResponse(request.id(), context.getCurrentTerm(), false, context.log().lastIndex());
    } else {
      return doAppendEntries(request);
    }
  }

  /**
   * Appends entries to the local log.
   */
  private AppendEntriesResponse doAppendEntries(AppendEntriesRequest request) {
    // If the log contains entries after the request's previous log index
    // then remove those entries to be replaced by the request entries.
    if (!request.entries().isEmpty()) {
      if (context.log().lastIndex() > request.prevLogIndex()) {
        for (int i = 0; i < request.entries().size(); i++) {
          RaftEntry entry = request.<RaftEntry>entries().get(i);
          RaftEntry match = context.log().getEntry(request.prevLogIndex() + i + 1);
          if (entry.term() != match.term()) {
            context.log().removeAfter(request.prevLogIndex() + i);
            context.log().appendEntries(request.entries().subList(i, request.entries().size()));
          }
        }
      } else {
        context.log().appendEntries(request.entries());
      }
    }
    return doApplyCommits(request);
  }

  /**
   * Applies commits to the local state machine.
   */
  private AppendEntriesResponse doApplyCommits(AppendEntriesRequest request) {
    // If the synced commit index is greater than the local commit index then
    // apply commits to the local state machine.
    // Also, it's possible that one of the previous command applications failed
    // due to asynchronous communication errors, so alternatively check if the
    // local commit index is greater than last applied. If all the state machine
    // commands have not yet been applied then we want to re-attempt to apply them.
    if (request.commitIndex() > context.getCommitIndex() || context.getCommitIndex() > context.getLastApplied()) {
      // Update the local commit index with min(request commit, last log // index)
      long lastIndex = context.log().lastIndex();
      context.setCommitIndex(Math.min(Math.max(request.commitIndex(), context.getCommitIndex()), lastIndex));

      // If the updated commit index indicates that commits remain to be
      // applied to the state machine, iterate entries and apply them.
      if (context.getCommitIndex() > context.getLastApplied()) {
        // Starting after the last applied entry, iterate through new entries
        // and apply them to the state machine up to the commit index.
        for (long i = context.getLastApplied() + 1; i <= Math.min(context.getCommitIndex(), lastIndex); i++) {
          // Apply the entry to the state machine.
          applyEntry(i);
        }

        // Once entries have been applied check whether we need to compact the log.
        compactLog();
      }
    }
    return new AppendEntriesResponse(request.id(), context.getCurrentTerm(), true, context.log().lastIndex());
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
    if (context.getLastApplied() == index-1) {
      // Ensure that the entry exists.
      if (entry == null) {
        throw new IllegalStateException("null entry cannot be applied to state machine");
      }
  
      // If the entry is a command entry, apply the command to the state machine.
      if (entry instanceof CommandEntry) {
        applyCommand(index, (CommandEntry) entry);
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
        context.setLastApplied(index);
      }
    }
  }

  /**
   * Applies a command entry to the state machine.
   *
   * @param index The index of the entry being applied.
   * @param entry The entry to apply.
   */
  protected void applyCommand(long index, CommandEntry entry) {
    try {
      context.stateMachine().applyCommand(entry.command(), entry.args());
    } catch (Exception e) {
    } finally {
      context.setLastApplied(index);
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
      Set<String> members = ((ConfigurationEntry) entry).cluster();
      members.remove(context.clusterConfig().getLocalMember());
      context.clusterConfig().setRemoteMembers(members);
    } catch (Exception e) {
    } finally {
      context.setLastApplied(index);
    }
  }

  /**
   * Applies a single snapshot entry to the state machine.
   *
   * @param index The index of the entry to apply.
   * @param entry The entry to apply.
   */
  protected void applySnapshot(long index, SnapshotEntry entry) {
    // Apply the snapshot to the local state machine.
    context.stateMachine().installSnapshot(entry.data());

    // If the log is compactable then compact it at the snapshot index.
    if (context.log() instanceof Compactable) {
      try {
        ((Compactable) context.log()).compact(index, entry);
      } catch (IOException e) {
        throw new CopyCatException("Failed to compact log.", e);
      }
    }

    // Set the local cluster configuration according to the snapshot cluster membership.
    Set<String> members = entry.cluster();
    members.remove(context.clusterConfig().getLocalMember());
    context.clusterConfig().setRemoteMembers(members);

    // Finally, if necessary, increment the current term.
    context.setCurrentTerm(Math.max(context.getCurrentTerm(), entry.term()));
    context.setLastApplied(index);
  }

  /**
   * Creates a snapshot of the state machine state.
   *
   * @return A snapshot of the state machine state.
   */
  protected SnapshotEntry createSnapshot() {
    byte[] snapshot = context.stateMachine().takeSnapshot();
    if (snapshot != null) {
      return new SnapshotEntry(context.getCurrentTerm(), context.clusterConfig().getMembers(), snapshot);
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
    if (context.log() instanceof Compactable && context.log().size() > context.config().getMaxLogSize()) {
      synchronized (context.log()) {
        final long lastApplied = context.getLastApplied();
        final SnapshotEntry snapshot = createSnapshot();
        if (snapshot != null) {
          Compactable log = (Compactable) context.log();
          try {
            log.compact(lastApplied, snapshot);
          } catch (IOException e) {
            throw new CopyCatException("Failed to compact log.", e);
          }
        }
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
    return CompletableFuture.supplyAsync(() -> {
      return handleRequestVote(request);
    }, executor);
  }

  /**
   * Handles a request vote request.
   */
  private RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getCurrentTerm()) {
      context.setCurrentTerm(request.term());
      context.setCurrentLeader(null);
      context.setLastVotedFor(null);
    }

    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < context.getCurrentTerm()) {
      return new RequestVoteResponse(request.id(), context.getCurrentTerm(), false);
    }
    // If the requesting candidate is ourself then always vote for ourself. Votes
    // for self are done by calling the local node. Note that this obviously
    // doesn't make sense for a leader.
    else if (request.candidate().equals(context.clusterConfig().getLocalMember())) {
      context.setLastVotedFor(context.clusterConfig().getLocalMember());
      context.events().voteCast().handle(new VoteCastEvent(context.getCurrentTerm(), context.clusterConfig().getLocalMember()));
      return new RequestVoteResponse(request.id(), context.getCurrentTerm(), true);
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (!context.clusterConfig().getMembers().contains(request.candidate())) {
      return new RequestVoteResponse(request.id(), context.getCurrentTerm(), false);
    }
    // If we've already voted for someone else then don't vote again.
    else if (context.getLastVotedFor() == null || context.getLastVotedFor().equals(request.candidate())) {
      // If the log is empty then vote for the candidate.
      if (context.log().isEmpty()) {
        context.setLastVotedFor(request.candidate());
        context.events().voteCast().handle(new VoteCastEvent(context.getCurrentTerm(), request.candidate()));
        return new RequestVoteResponse(request.id(), context.getCurrentTerm(), true);
      } else {
        // Otherwise, load the last entry in the log. The last entry should be
        // at least as up to date as the candidates entry and term.
        long lastIndex = context.log().lastIndex();
        RaftEntry entry = context.log().getEntry(lastIndex);
        if (entry == null) {
          context.setLastVotedFor(request.candidate());
          context.events().voteCast().handle(new VoteCastEvent(context.getCurrentTerm(), request.candidate()));
          return new RequestVoteResponse(request.id(), context.getCurrentTerm(), true);
        }

        long lastTerm = entry.term();
        if (request.lastLogIndex() >= lastIndex && request.lastLogTerm() >= lastTerm) {
          context.setLastVotedFor(request.candidate());
          context.events().voteCast().handle(new VoteCastEvent(context.getCurrentTerm(), request.candidate()));
          return new RequestVoteResponse(request.id(), context.getCurrentTerm(), true);
        } else {
          context.setLastVotedFor(null);
          return new RequestVoteResponse(request.id(), context.getCurrentTerm(), false);
        }
      }
    }
    // In this case, we've already voted for someone else.
    else {
      return new RequestVoteResponse(request.id(), context.getCurrentTerm(), false);
    }
  }

  @Override
  public CompletableFuture<SubmitCommandResponse> submitCommand(SubmitCommandRequest request) {
    return CompletableFuture.completedFuture(new SubmitCommandResponse(request.id(), "Not the leader"));
  }

  @Override
  public void destroy() {
    context.cluster().localMember().protocol().server().protocolHandler(null);
  }

}
