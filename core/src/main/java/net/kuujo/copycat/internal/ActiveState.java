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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.log.ActionEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract active state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class ActiveState extends AbstractState {
  private final AtomicBoolean transition = new AtomicBoolean();

  protected ActiveState(CopycatStateContext context) {
    super(context);
  }

  /**
   * Transitions to a new state.
   */
  protected CompletableFuture<CopycatState> transition(CopycatState state) {
    if (transitionHandler != null) {
      return transitionHandler.handle(state);
    }
    return exceptionalFuture(new IllegalStateException("No transition handler registered"));
  }

  @Override
  public CompletableFuture<PingResponse> ping(final PingRequest request) {
    CompletableFuture<PingResponse> future = CompletableFuture.completedFuture(logResponse(handlePing(logRequest(request))));
    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition.get()) {
      transition(CopycatState.FOLLOWER);
    }
    return future;
  }

  /**
   * Handles a ping request.
   */
  private PingResponse handlePing(PingRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == null)) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
      transition.set(true);
    }

    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getTerm()) {
      logger().warn("{} - Rejected {}: request term is less than the current term ({})", context.getLocalMember(), request, context
        .getTerm());
      return PingResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .build();
    } else if (request.logIndex() > 0 && request.logTerm() > 0) {
      return doCheckPingEntry(request);
    }
    return PingResponse.builder()
      .withId(request.id())
      .withMember(context.getLocalMember())
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .build();
  }

  /**
   * Checks the ping log entry for consistency.
   */
  private PingResponse doCheckPingEntry(PingRequest request) {
    if (request.logIndex() > context.log().lastIndex()) {
      logger().warn("{} - Rejected {}: previous index ({}) is greater than the local log's last index ({})", context.getLocalMember(), request, request.logIndex(), context.log().lastIndex());
      return PingResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .build();
    }

    // If the log entry exists then load the entry.
    // If the last log entry's term is not the same as the given
    // prevLogTerm then return false. This will cause the leader to
    // decrement this node's nextIndex and ultimately retry with the
    // leader's previous log entry so that the inconsistent entry
    // can be overwritten.
    Entry entry = context.log().getEntry(request.logIndex());
    if (entry == null) {
      logger().warn("{} - Rejected {}: request entry not found in local log", context.getLocalMember(), request);
      return PingResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .build();
    } else if (entry.term() != request.logTerm()) {
      logger().warn("{} - Rejected {}: request entry term does not match local log", context.getLocalMember(), request);
      return PingResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .build();
    } else {
      doApplyCommits(request.commitIndex());
      return PingResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(true)
        .build();
    }
  }

  @Override
  public synchronized CompletableFuture<SyncResponse> sync(final SyncRequest request) {
    CompletableFuture<SyncResponse> future = CompletableFuture.completedFuture(logResponse(handleSync(logRequest(request))));
    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition.get()) {
      transition(CopycatState.FOLLOWER);
    }
    return future;
  }

  /**
   * Starts the sync process.
   */
  private synchronized SyncResponse handleSync(SyncRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == null)) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
      transition.set(true);
    }

    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getTerm()) {
      logger().warn("{} - Rejected {}: request term is less than the current term ({})", context.getLocalMember(), request, context.getTerm());
      return SyncResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build();
    } else if (request.logIndex() > 0 && request.logTerm() > 0) {
      return doCheckPreviousEntry(request);
    } else {
      return doAppendEntries(request);
    }
  }

  /**
   * Checks the previous log entry for consistency.
   */
  private synchronized SyncResponse doCheckPreviousEntry(SyncRequest request) {
    if (request.logIndex() > context.log().lastIndex()) {
      logger().warn("{} - Rejected {}: previous index ({}) is greater than the local log's last index ({})", context.getLocalMember(), request, request.logIndex(), context.log().lastIndex());
      return SyncResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build();
    }

    // If the log entry exists then load the entry.
    // If the last log entry's term is not the same as the given
    // prevLogTerm then return false. This will cause the leader to
    // decrement this node's nextIndex and ultimately retry with the
    // leader's previous log entry so that the inconsistent entry
    // can be overwritten.
    Entry entry = context.log().getEntry(request.logIndex());
    if (entry == null) {
      logger().warn("{} - Rejected {}: request entry not found in local log", context.getLocalMember(), request);
      return SyncResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build();
    } else if (entry.term() != request.logTerm()) {
      logger().warn("{} - Rejected {}: request entry term does not match local log", context.getLocalMember(), request);
      return SyncResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build();
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
        long index = request.logIndex();
        for (Entry entry : request.entries()) {
          index++;
          // Replicated snapshot entries are *always* immediately logged and applied to the state machine
          // since snapshots are only taken of committed state machine state. This will cause all previous
          // entries to be removed from the log.
          Entry match = context.log().getEntry(index);
          if (match != null) {
            if (entry.term() != match.term()) {
              logger().warn("{} - Synced entry does not match local log, removing incorrect entries", context.getLocalMember());
              context.log().removeAfter(index - 1);
              context.log().appendEntry(entry);
              logger().debug("{} - Appended {} to log at index {}", context.getLocalMember(), entry, index);
            }
          } else {
            context.log().appendEntry(entry);
            logger().debug("{} - Appended {} to log at index {}", context.getLocalMember(), entry, index);
          }
        }
      }
    }
    doApplyCommits(request.commitIndex());
    return SyncResponse.builder()
      .withId(request.id())
      .withMember(context.getLocalMember())
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(context.log().lastIndex())
      .build();
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
    if (commitIndex > context.getCommitIndex() || context.getCommitIndex() > context.getLastApplied()) {
      // Update the local commit index with min(request commit, last log // index)
      long lastIndex = context.log().lastIndex();
      context.setCommitIndex(Math.min(Math.max(commitIndex, context.getCommitIndex()), lastIndex));

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
  }

  /**
   * Applies the given entry.
   */
  @SuppressWarnings("unchecked")
  protected void applyEntry(long index) {
    if (context.getLastApplied() == index-1) {
      Entry entry = context.log().getEntry(index);

      // Ensure that the entry exists.
      if (entry == null) {
        throw new IllegalStateException("null entry cannot be applied to state machine");
      }

      if (entry instanceof ConfigurationEntry) {
        context.setMembers(((ConfigurationEntry) entry).members());
      } else if (entry instanceof ActionEntry) {
        ActionEntry actionEntry = (ActionEntry) entry;
        ActionInfo action = context.action(actionEntry.action());
        if (action != null) {
          try {
            action.action.execute(index, actionEntry.entry());
          } catch (Exception e) {
            logger().warn(e.getMessage());
          }
        }
      }
      context.setLastApplied(index);
    }
  }

  /**
   * Compacts the log.
   */
  protected void compactLog() {
    // TODO
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    logger().debug("{} - Received {}", context.getLocalMember(), request);
    return CompletableFuture.completedFuture(logResponse(handlePoll(logRequest(request))));
  }

  /**
   * Handles a vote request.
   */
  private PollResponse handlePoll(PollRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm()) {
      context.setTerm(request.term());
      context.setLeader(null);
      context.setLastVotedFor(null);
    }

    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < context.getTerm()) {
      logger().debug("{} - Rejected {}: candidate's term is less than the current term", context.getLocalMember(), request);
      return PollResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If the requesting candidate is ourself then always vote for ourself. Votes
    // for self are done by calling the local node. Note that this obviously
    // doesn't make sense for a leader.
    else if (request.candidate().equals(context.getLocalMember())) {
      context.setLastVotedFor(context.getLocalMember());
      logger().debug("{} - Accepted {}: candidate is the local node", context.getLocalMember(), request);
      return PollResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withVoted(true)
        .build();
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (!context.getMembers().contains(request.candidate())) {
      logger().debug("{} - Rejected {}: candidate is not known do the local node", context.getLocalMember(), request);
      return PollResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If we've already voted for someone else then don't vote again.
    else if (context.getLastVotedFor() == null || context.getLastVotedFor().equals(request.candidate())) {
      // If the log is empty then vote for the candidate.
      if (context.log().isEmpty()) {
        context.setLastVotedFor(request.candidate());
        logger().debug("{} - Accepted {}: candidate's log is up-to-date", context.getLocalMember(), request);
        return PollResponse.builder()
          .withId(request.id())
          .withMember(context.getLocalMember())
          .withTerm(context.getTerm())
          .withVoted(true)
          .build();
      } else {
        // Otherwise, load the last entry in the log. The last entry should be
        // at least as up to date as the candidates entry and term.
        long lastIndex = context.log().lastIndex();
        Entry entry = context.log().getEntry(lastIndex);
        if (entry == null) {
          context.setLastVotedFor(request.candidate());
          logger().debug("{} - Accepted {}: candidate's log is up-to-date", context.getLocalMember(), request);
          return PollResponse.builder()
            .withId(request.id())
            .withMember(context.getLocalMember())
            .withTerm(context.getTerm())
            .withVoted(true)
            .build();
        }

        long lastTerm = entry.term();
        if (request.logIndex() >= lastIndex) {
          if (request.logTerm() >= lastTerm) {
            context.setLastVotedFor(request.candidate());
            logger().debug("{} - Accepted {}: candidate's log is up-to-date", context.getLocalMember(), request);
            return PollResponse.builder()
              .withId(request.id())
              .withMember(context.getLocalMember())
              .withTerm(context.getTerm())
              .withVoted(true)
              .build();
          } else {
            logger().debug("{} - Rejected {}: candidate's last log term ({}) is in conflict with local log ({})", context.getLocalMember(), request, request.logTerm(), lastTerm);
            return PollResponse.builder()
              .withId(request.id())
              .withMember(context.getLocalMember())
              .withTerm(context.getTerm())
              .withVoted(false)
              .build();
          }
        } else {
          logger().debug("{} - Rejected {}: candidate's last log entry ({}) is at a lower index than the local log ({})", context.getLocalMember(), request, request.logIndex(), lastIndex);
          return PollResponse.builder()
            .withId(request.id())
            .withMember(context.getLocalMember())
            .withTerm(context.getTerm())
            .withVoted(false)
            .build();
        }
      }
    }
    // In this case, we've already voted for someone else.
    else {
      logger().debug("{} - Rejected {}: already voted for {}", context.getLocalMember(), request, context.getLastVotedFor());
      return PollResponse.builder()
        .withId(request.id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
  }

  @Override
  public CompletableFuture<CommitResponse> commit(CommitRequest request) {
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(CommitResponse.builder()
      .withId(request.id())
      .withMember(context.getLocalMember())
      .withStatus(Response.Status.ERROR)
      .withError(new IllegalStateException("Not the leader"))
      .build()));
  }

}
