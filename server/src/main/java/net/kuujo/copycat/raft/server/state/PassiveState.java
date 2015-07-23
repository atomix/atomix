/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft.server.state;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.raft.RaftError;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.raft.server.RaftServer;
import net.kuujo.copycat.raft.server.log.RaftEntry;
import net.kuujo.copycat.util.concurrent.ComposableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Passive state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class PassiveState extends AbstractState {
  protected boolean transition;

  public PassiveState(RaftServerState context) {
    super(context);
  }

  @Override
  public RaftServer.State type() {
    return RaftServer.State.PASSIVE;
  }

  /**
   * Transitions to a new state.
   */
  protected void transition(RaftServer.State state) {
    // Do not allow the PASSIVE state to transition.
  }

  @Override
  protected CompletableFuture<AppendResponse> append(final AppendRequest request) {
    context.checkThread();
    CompletableFuture<AppendResponse> future = CompletableFuture.completedFuture(logResponse(handleAppend(logRequest(request))));
    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition) {
      transition(RaftServer.State.FOLLOWER);
      transition = false;
    }
    return future;
  }

  /**
   * Starts the append process.
   */
  private AppendResponse handleAppend(AppendRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == 0)) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
      transition = true;
    }

    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getTerm()) {
      LOGGER.warn("{} - Rejected {}: request term is less than the current term ({})", context.getMemberId(), request, context.getTerm());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    } else if (request.logIndex() != 0 && request.logTerm() != 0) {
      return doCheckPreviousEntry(request);
    } else {
      return doAppendEntries(request);
    }
  }

  /**
   * Checks the previous log entry for consistency.
   */
  private AppendResponse doCheckPreviousEntry(AppendRequest request) {
    if (request.logIndex() != 0 && context.getLog().isEmpty()) {
      LOGGER.warn("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getMemberId(), request, request.logIndex(), context.getLog().lastIndex());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    } else if (request.logIndex() != 0 && context.getLog().lastIndex() != 0 && request.logIndex() > context.getLog().lastIndex()) {
      LOGGER.warn("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getMemberId(), request, request.logIndex(), context.getLog().lastIndex());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    }

    // If the previous entry term doesn't match the local previous term then reject the request.
    RaftEntry entry = context.getLog().getEntry(request.logIndex());
    if (entry == null || entry.getTerm() != request.logTerm()) {
      LOGGER.warn("{} - Rejected {}: Request log term does not match local log term {} for the same entry", context.getMemberId(), request, entry.getTerm());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(request.logIndex() <= context.getLog().lastIndex() ? request.logIndex() - 1 : context.getLog().lastIndex())
        .build();
    } else {
      return doAppendEntries(request);
    }
  }

  /**
   * Appends entries to the local log.
   */
  @SuppressWarnings("unchecked")
  private AppendResponse doAppendEntries(AppendRequest request) {
    // If the log contains entries after the request's previous log index
    // then remove those entries to be replaced by the request entries.
    if (!request.entries().isEmpty()) {

      // Iterate through request entries and append them to the log.
      for (RaftEntry entry : (Iterable<RaftEntry>) request.entries()) {
        // If the entry index is greater than the last log index, skip missing entries.
        if (!context.getLog().containsIndex(entry.getIndex())) {
          context.getLog().skip(entry.getIndex() - context.getLog().lastIndex() - 1).appendEntry(entry);
          LOGGER.debug("{} - Appended {} to log at index {}", context.getMemberId(), entry, entry.getIndex());
        } else {
          // Compare the term of the received entry with the matching entry in the log.
          RaftEntry match = context.getLog().getEntry(entry.getIndex());
          if (match != null) {
            if (entry.getTerm() != match.getTerm()) {
              // We found an invalid entry in the log. Remove the invalid entry and append the new entry.
              // If appending to the log fails, apply commits and reply false to the append request.
              LOGGER.warn("{} - Appended entry term does not match local log, removing incorrect entries", context.getMemberId());
              context.getLog().truncate(entry.getIndex() - 1);
              context.getLog().appendEntry(entry);
              LOGGER.debug("{} - Appended {} to log at index {}", context.getMemberId(), entry, entry.getIndex());
            }
          } else {
            context.getLog().truncate(entry.getIndex() - 1).appendEntry(entry);
            LOGGER.debug("{} - Appended {} to log at index {}", context.getMemberId(), entry, entry.getIndex());
          }
        }
      }
    }

    // If we've made it this far, apply commits and send a successful response.
    context.getContext().execute(() -> {
      applyCommits(request.commitIndex())
        .thenRun(() -> applyIndex(request.globalIndex()));
    });

    return AppendResponse.builder()
      .withStatus(Response.Status.OK)
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(context.getLog().lastIndex())
      .build();
  }

  /**
   * Applies commits to the local state machine.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Void> applyCommits(long commitIndex) {
    // Set the commit index, ensuring that the index cannot be decreased.
    context.setCommitIndex(Math.max(context.getCommitIndex(), commitIndex));

    // The entries to be applied to the state machine are the difference between min(lastIndex, commitIndex) and lastApplied.
    long lastIndex = context.getLog().lastIndex();
    long lastApplied = context.getStateMachine().getLastApplied();

    long effectiveIndex = Math.min(lastIndex, context.getCommitIndex());

    // If the effective commit index is greater than the last index applied to the state machine then apply remaining entries.
    if (effectiveIndex > lastApplied) {
      long entriesToApply = effectiveIndex - lastApplied;
      LOGGER.debug("{} - Applying {} commits", context.getMemberId(), entriesToApply);

      // Rather than composing all futures into a single future, use a counter to count completions in order to preserve memory.
      AtomicLong counter = new AtomicLong();
      CompletableFuture<Void> future = new CompletableFuture<>();

      for (long i = lastApplied + 1; i <= effectiveIndex; i++) {
        Entry entry = context.getLog().getEntry(i);
        if (entry != null) {
          applyEntry(entry).whenCompleteAsync((result, error) -> {
            entry.close();
            if (isOpen() && error != null) {
              LOGGER.info("{} - An application error occurred: {}", context.getMemberId(), error.getMessage());
            }
            if (counter.incrementAndGet() == entriesToApply) {
              future.complete(null);
            }
          }, context.getContext());
        }
      }
      return future;
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies an entry to the state machine.
   */
  protected CompletableFuture<?> applyEntry(Entry entry) {
    LOGGER.debug("{} - Applying {}", context.getMemberId(), entry);
    return context.getStateMachine().apply(entry);
  }

  /**
   * Recycles the log up to the given index.
   */
  protected void applyIndex(long globalIndex) {
    if (globalIndex > 0) {
      context.setGlobalIndex(globalIndex);
    }
  }

  @Override
  protected CompletableFuture<PollResponse> poll(PollRequest request) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<CommandResponse> command(CommandRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getConnections()
        .getConnection(context.getMembers().member(context.getLeader()))
        .thenCompose(connection -> connection.send(request));
    }
  }

  @Override
  protected CompletableFuture<QueryResponse> query(QueryRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getConnections()
        .getConnection(context.getMembers().member(context.getLeader()))
        .thenCompose(connection -> connection.send(request));
    }
  }

  @Override
  protected CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(RegisterResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getConnections()
        .getConnection(context.getMembers().member(context.getLeader()))
        .thenCompose(connection -> connection.send(request));
    }
  }

  @Override
  protected CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(KeepAliveResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.getConnections()
        .getConnection(context.getMembers().member(context.getLeader()))
        .thenCompose(connection -> connection.send(request));
    }
  }

  @Override
  protected CompletableFuture<PublishResponse> publish(PublishRequest request) {
    context.checkThread();
    logRequest(request);

    Session session = context.getSession();
    if (session != null) {
      CompletableFuture<PublishResponse> future = new ComposableFuture<>();
      session.publish(request.message()).whenCompleteAsync((result, error) -> {
        if (error == null) {
          future.complete(logResponse(PublishResponse.builder()
            .withStatus(Response.Status.OK)
            .build()));
        } else {
          future.complete(logResponse(PublishResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build()));
        }
      }, context.getContext());
      return future;
    } else {
      return CompletableFuture.completedFuture(logResponse(PublishResponse.builder()
        .withStatus(Response.Status.OK)
        .build()));
    }
  }

}
