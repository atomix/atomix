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
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.raft.server.RaftServer;
import net.kuujo.copycat.raft.server.log.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Leader state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class LeaderState extends ActiveState {
  private static final int MAX_BATCH_SIZE = 1024 * 32;
  private ScheduledFuture<?> currentTimer;
  private final Replicator replicator = new Replicator();

  public LeaderState(RaftServerState context) {
    super(context);
  }

  @Override
  public RaftServer.State type() {
    return RaftServer.State.LEADER;
  }

  @Override
  public synchronized CompletableFuture<AbstractState> open() {
    // Schedule the initial entries commit to occur after the state is opened. Attempting any communication
    // within the open() method will result in a deadlock since RaftProtocol calls this method synchronously.
    // What is critical about this logic is that the heartbeat timer not be started until a NOOP entry has been committed.
    context.getContext().execute(() -> {
      commitEntries().whenComplete((result, error) -> {
        if (error == null) {
          startHeartbeatTimer();
        }
      });
    });

    return super.open()
      .thenRun(this::takeLeadership)
      .thenApply(v -> this);
  }

  /**
   * Sets the current node as the cluster leader.
   */
  private void takeLeadership() {
    context.setLeader(context.getMemberId());
  }

  /**
   * Commits a no-op entry to the log, ensuring any entries from a previous term are committed.
   */
  private CompletableFuture<Void> commitEntries() {
    final long term = context.getTerm();
    final long index;
    try (NoOpEntry entry = context.getLog().createEntry(NoOpEntry.class)) {
      entry.setTerm(term);
      index = context.getLog().appendEntry(entry);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    replicator.commit(index).whenComplete((resultIndex, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          applyEntries(resultIndex);
          future.complete(null);
        } else {
          transition(RaftServer.State.FOLLOWER);
        }
      }
    });
    return future;
  }

  /**
   * Applies all unapplied entries to the log.
   */
  private void applyEntries(long index) {
    if (!context.getLog().isEmpty()) {
      int count = 0;
      for (long lastApplied = Math.max(context.getStateMachine().getLastApplied(), context.getLog().firstIndex()); lastApplied <= index; lastApplied++) {
        try (Entry entry = context.getLog().getEntry(lastApplied)) {
          if (entry != null) {
            context.getStateMachine().apply(entry).whenCompleteAsync((result, error) -> {
              if (isOpen() && error != null) {
                LOGGER.info("{} - An application error occurred: {}", context.getMemberId(), error);
              }
              entry.close();
            }, context.getContext());
          }
        }
        count++;
      }
      LOGGER.debug("{} - Applied {} entries to log", context.getMemberId(), count);
    }
  }

  /**
   * Starts heartbeating all cluster members.
   */
  private void startHeartbeatTimer() {
    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
    LOGGER.debug("{} - Starting heartbeat timer", context.getMemberId());
    currentTimer = context.getContext().scheduleAtFixedRate(this::heartbeatMembers, 0, context.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
  }

  /**
   * Sends a heartbeat to all members of the cluster.
   */
  private void heartbeatMembers() {
    context.checkThread();
    if (isOpen()) {
      replicator.commit();
    }
  }

  @Override
  public CompletableFuture<PollResponse> poll(final PollRequest request) {
    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
      .withStatus(Response.Status.OK)
      .withTerm(context.getTerm())
      .withAccepted(false)
      .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(final VoteRequest request) {
    if (request.term() > context.getTerm()) {
      LOGGER.debug("{} - Received greater term", context.getMemberId());
      context.setLeader(0);
      transition(RaftServer.State.FOLLOWER);
      return super.vote(request);
    } else {
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build()));
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(final AppendRequest request) {
    context.checkThread();
    if (request.term() > context.getTerm()) {
      return super.append(request);
    } else if (request.term() < context.getTerm()) {
      return CompletableFuture.completedFuture(logResponse(AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build()));
    } else {
      context.setLeader(request.leader());
      transition(RaftServer.State.FOLLOWER);
      return super.append(request);
    }
  }

  @Override
  protected CompletableFuture<CommandResponse> command(final CommandRequest request) {
    context.checkThread();
    logRequest(request);

    Command command = request.command();
    final long term = context.getTerm();
    final long timestamp = System.currentTimeMillis();
    final long index;

    try (CommandEntry entry = context.getLog().createEntry(CommandEntry.class)) {
      entry.setTerm(term)
        .setSession(request.session())
        .setRequest(request.request())
        .setResponse(request.response())
        .setTimestamp(timestamp)
        .setCommand(command);
      index = context.getLog().appendEntry(entry);
      LOGGER.debug("{} - Appended entry to log at index {}", context.getMemberId(), index);
    }

    CompletableFuture<CommandResponse> future = new CompletableFuture<>();
    replicator.commit(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          CommandEntry entry = context.getLog().getEntry(index);
          if (entry != null) {
            applyEntry(entry).whenCompleteAsync((result, error) -> {
              if (isOpen()) {
                if (error == null) {
                  future.complete(logResponse(CommandResponse.builder()
                    .withStatus(Response.Status.OK)
                    .withResult(result)
                    .build()));
                } else if (error instanceof RaftException) {
                  future.complete(logResponse(CommandResponse.builder()
                    .withStatus(Response.Status.ERROR)
                    .withError(((RaftException) error).getType())
                    .build()));
                } else {
                  future.complete(logResponse(CommandResponse.builder()
                    .withStatus(Response.Status.ERROR)
                    .withError(RaftError.Type.INTERNAL_ERROR)
                    .build()));
                }
              }
              entry.close();
            }, context.getContext());
          } else {
            future.complete(logResponse(CommandResponse.builder()
              .withStatus(Response.Status.OK)
              .withResult(null)
              .build()));
          }
        } else {
          future.complete(logResponse(CommandResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.PROTOCOL_ERROR)
            .build()));
        }
      }
    });
    return future;
  }

  @Override
  protected CompletableFuture<QueryResponse> query(final QueryRequest request) {
    Query query = request.query();

    final long timestamp = System.currentTimeMillis();
    final long index = context.getCommitIndex();

    QueryEntry entry = context.getLog().createEntry(QueryEntry.class)
      .setIndex(index)
      .setTerm(context.getTerm())
      .setSession(request.session())
      .setVersion(request.version())
      .setTimestamp(timestamp)
      .setQuery(query);

    ConsistencyLevel consistency = query.consistency();
    if (consistency == null)
      return submitQueryLinearizableStrict(entry);

    switch (consistency) {
      case SERIALIZABLE:
        return submitQuerySerializable(entry);
      case LINEARIZABLE_LEASE:
        return submitQueryLinearizableLease(entry);
      case LINEARIZABLE:
        return submitQueryLinearizableStrict(entry);
      default:
        throw new IllegalStateException("unknown consistency level");
    }
  }

  /**
   * Submits a query with serializable consistency.
   */
  private CompletableFuture<QueryResponse> submitQuerySerializable(QueryEntry entry) {
    return applyQuery(entry, new CompletableFuture<>());
  }

  /**
   * Submits a query with lease based linearizable consistency.
   */
  private CompletableFuture<QueryResponse> submitQueryLinearizableLease(QueryEntry entry) {
    long commitTime = replicator.commitTime();
    if (System.currentTimeMillis() - commitTime < context.getElectionTimeout()) {
      return submitQuerySerializable(entry);
    } else {
      return submitQueryLinearizableStrict(entry);
    }
  }

  /**
   * Submits a query with strict linearizable consistency.
   */
  private CompletableFuture<QueryResponse> submitQueryLinearizableStrict(QueryEntry entry) {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    replicator.commit().whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          applyQuery(entry, future);
        } else {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.COMMAND_ERROR)
            .build()));
        }
      }
      entry.close();
    });
    return future;
  }

  /**
   * Applies a query to the state machine.
   */
  private CompletableFuture<QueryResponse> applyQuery(QueryEntry entry, CompletableFuture<QueryResponse> future) {
    long version = context.getStateMachine().getLastApplied();
    context.getStateMachine().apply(entry).whenCompleteAsync((result, error) -> {
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.OK)
            .withVersion(version)
            .withResult(result)
            .build()));
        } else if (error instanceof RaftException) {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(((RaftException) error).getType())
            .build()));
        } else {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build()));
        }
      }
      entry.close();
    }, context.getContext());
    return future;
  }

  @Override
  protected CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    context.checkThread();
    logRequest(request);

    final long timestamp = System.currentTimeMillis();
    final long index;

    try (RegisterEntry entry = context.getLog().createEntry(RegisterEntry.class)) {
      entry.setTerm(context.getTerm());
      entry.setTimestamp(timestamp);
      entry.setMember(request.member());
      entry.setConnection(request.connection());
      index = context.getLog().appendEntry(entry);
      LOGGER.debug("{} - Appended {}", context.getMemberId(), entry);
    }

    CompletableFuture<RegisterResponse> future = new CompletableFuture<>();
    replicator.commit(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          RegisterEntry entry = context.getLog().getEntry(index);
          applyEntry(entry).whenCompleteAsync((sessionId, sessionError) -> {
            if (isOpen()) {
              if (sessionError == null) {
                future.complete(logResponse(RegisterResponse.builder()
                  .withStatus(Response.Status.OK)
                  .withLeader(context.getLeader())
                  .withTerm(context.getTerm())
                  .withSession((Long) sessionId)
                  .withMembers(context.getMembers())
                  .build()));
              } else if (sessionError instanceof RaftException) {
                future.complete(logResponse(RegisterResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(((RaftException) sessionError).getType())
                  .build()));
              } else {
                future.complete(logResponse(RegisterResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(RaftError.Type.INTERNAL_ERROR)
                  .build()));
              }
            }
            entry.close();
          }, context.getContext());
        } else {
          future.complete(logResponse(RegisterResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.PROTOCOL_ERROR)
            .build()));
        }
      }
    });
    return future;
  }

  @Override
  protected CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    context.checkThread();
    logRequest(request);

    final long timestamp = System.currentTimeMillis();
    final long index;

    try (KeepAliveEntry entry = context.getLog().createEntry(KeepAliveEntry.class)) {
      entry.setTerm(context.getTerm());
      entry.setSession(request.session());
      entry.setTimestamp(timestamp);
      index = context.getLog().appendEntry(entry);
      LOGGER.debug("{} - Appended {}", context.getMemberId(), entry);
    }

    CompletableFuture<KeepAliveResponse> future = new CompletableFuture<>();
    replicator.commit(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          KeepAliveEntry entry = context.getLog().getEntry(index);
          long version = context.getStateMachine().getLastApplied();
          applyEntry(entry).whenCompleteAsync((sessionResult, sessionError) -> {
            if (isOpen()) {
              if (sessionError == null) {
                future.complete(logResponse(KeepAliveResponse.builder()
                  .withStatus(Response.Status.OK)
                  .withLeader(context.getLeader())
                  .withTerm(context.getTerm())
                  .withVersion(version)
                  .withMembers(context.getMembers())
                  .build()));
              } else if (sessionError instanceof RaftException) {
                future.complete(logResponse(KeepAliveResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(((RaftException) sessionError).getType())
                  .build()));
              } else {
                future.complete(logResponse(KeepAliveResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(RaftError.Type.INTERNAL_ERROR)
                  .build()));
              }
            }
            entry.close();
          }, context.getContext());
        } else {
          future.complete(logResponse(KeepAliveResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.PROTOCOL_ERROR)
            .build()));
        }
      }
    });
    return future;
  }

  @Override
  protected CompletableFuture<JoinResponse> join(JoinRequest request) {
    context.checkThread();
    logRequest(request);

    final long index;

    try (JoinEntry entry = context.getLog().createEntry(JoinEntry.class)) {
      entry.setTerm(context.getTerm());
      entry.setMember(request.member());
      index = context.getLog().appendEntry(entry);
      LOGGER.debug("{} - Appended {}", context.getMemberId(), entry);
    }

    CompletableFuture<JoinResponse> future = new CompletableFuture<>();
    replicator.commit(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          JoinEntry entry = context.getLog().getEntry(index);
          applyEntry(entry).whenCompleteAsync((sessionId, sessionError) -> {
            if (isOpen()) {
              if (sessionError == null) {
                future.complete(logResponse(JoinResponse.builder()
                  .withStatus(Response.Status.OK)
                  .withLeader(context.getLeader())
                  .withTerm(context.getTerm())
                  .build()));
              } else {
                future.complete(logResponse(JoinResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(RaftError.Type.INTERNAL_ERROR)
                  .build()));
              }
            }
            entry.close();
          }, context.getContext());
        } else {
          future.complete(logResponse(JoinResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.PROTOCOL_ERROR)
            .build()));
        }
      }
    });
    return future;
  }

  @Override
  protected CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    context.checkThread();
    logRequest(request);

    final long index;

    try (LeaveEntry entry = context.getLog().createEntry(LeaveEntry.class)) {
      entry.setTerm(context.getTerm());
      entry.setMember(request.member());
      index = context.getLog().appendEntry(entry);
      LOGGER.debug("{} - Appended {}", context.getMemberId(), entry);
    }

    CompletableFuture<LeaveResponse> future = new CompletableFuture<>();
    replicator.commit(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          LeaveEntry entry = context.getLog().getEntry(index);
          applyEntry(entry).whenCompleteAsync((sessionId, sessionError) -> {
            if (isOpen()) {
              if (sessionError == null) {
                future.complete(logResponse(LeaveResponse.builder()
                  .withStatus(Response.Status.OK)
                  .build()));
              } else {
                future.complete(logResponse(LeaveResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(RaftError.Type.INTERNAL_ERROR)
                  .build()));
              }
            }
            entry.close();
          }, context.getContext());
        } else {
          future.complete(logResponse(LeaveResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.PROTOCOL_ERROR)
            .build()));
        }
      }
    });
    return future;
  }

  /**
   * Cancels the ping timer.
   */
  private void cancelPingTimer() {
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling heartbeat timer", context.getMemberId());
      currentTimer.cancel(false);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelPingTimer);
  }

  /**
   * Log replicator.
   */
  private class Replicator {
    private final List<Replica> replicas = new ArrayList<>();
    private final List<Long> commitTimes = new ArrayList<>();
    private long commitTime;
    private CompletableFuture<Long> commitFuture;
    private CompletableFuture<Long> nextCommitFuture;
    private final TreeMap<Long, CompletableFuture<Long>> commitFutures = new TreeMap<>();
    private int quorum;
    private int quorumIndex;

    private Replicator() {
      Set<Member> members = context.getMembers().members().stream()
        .filter(m -> m.id() != context.getMemberId() && m.type() == Member.Type.ACTIVE)
        .collect(Collectors.toSet());
      for (Member member : members) {
        this.replicas.add(new Replica(this.replicas.size(), member));
        this.commitTimes.add(System.currentTimeMillis());
      }

      this.quorum = (int) Math.floor((this.replicas.size() + 1) / 2.0);
      this.quorumIndex = quorum - 1;
    }

    /**
     * Triggers a commit.
     *
     * @return A completable future to be completed the next time entries are committed to a majority of the cluster.
     */
    private CompletableFuture<Long> commit() {
      if (replicas.isEmpty())
        return CompletableFuture.completedFuture(null);

      if (commitFuture == null) {
        commitFuture = new CompletableFuture<>();
        commitTime = System.currentTimeMillis();
        replicas.forEach(Replica::commit);
        return commitFuture;
      } else if (nextCommitFuture == null) {
        nextCommitFuture = new CompletableFuture<>();
        return nextCommitFuture;
      } else {
        return nextCommitFuture;
      }
    }

    /**
     * Registers a commit handler for the given commit index.
     *
     * @param index The index for which to register the handler.
     * @return A completable future to be completed once the given log index has been committed.
     */
    private CompletableFuture<Long> commit(long index) {
      if (index == 0)
        return commit();

      if (replicas.isEmpty()) {
        context.setCommitIndex(index);
        return CompletableFuture.completedFuture(index);
      }

      return commitFutures.computeIfAbsent(index, i -> {
        replicas.forEach(Replica::commit);
        return new CompletableFuture<>();
      });
    }

    /**
     * Returns the last time a majority of the cluster was contacted.
     */
    private long commitTime() {
      Collections.sort(commitTimes, Collections.reverseOrder());
      return commitTimes.get(quorumIndex);
    }

    /**
     * Sets a commit time.
     */
    private void commitTime(int id) {
      commitTimes.set(id, System.currentTimeMillis());

      // Sort the list of commit times. Use the quorum index to get the last time the majority of the cluster
      // was contacted. If the current commitFuture's time is less than the commit time then trigger the
      // commit future and reset it to the next commit future.
      long commitTime = commitTime();
      if (commitFuture != null && this.commitTime <= commitTime) {
        commitFuture.complete(null);
        commitFuture = nextCommitFuture;
        nextCommitFuture = null;
        if (commitFuture != null) {
          this.commitTime = System.currentTimeMillis();
          replicas.forEach(Replica::commit);
        }
      }
    }

    /**
     * Checks whether any futures can be completed.
     */
    private void commitEntries() {
      context.checkThread();

      // Sort the list of replicas, order by the last index that was replicated
      // to the replica. This will allow us to determine the median index
      // for all known replicated entries across all cluster members.
      Collections.sort(replicas, (o1, o2) -> Long.compare(o2.state.getMatchIndex() != 0 ? o2.state.getMatchIndex() : 0l, o1.state.getMatchIndex() != 0 ? o1.state.getMatchIndex() : 0l));

      // Set the current commit index as the median replicated index.
      // Since replicas is a list with zero based indexes, use the negation of
      // the required quorum size to get the index of the replica with the least
      // possible quorum replication. That replica's match index is the commit index.
      // Set the commit index. Once the commit index has been set we can run
      // all tasks up to the given commit.
      long commitIndex = replicas.get(quorumIndex).state.getMatchIndex();
      long globalIndex = replicas.get(replicas.size() - 1).state.getMatchIndex();
      if (commitIndex > 0) {
        context.setCommitIndex(commitIndex);
        context.setGlobalIndex(globalIndex);
        SortedMap<Long, CompletableFuture<Long>> futures = commitFutures.headMap(commitIndex, true);
        for (Map.Entry<Long, CompletableFuture<Long>> entry : futures.entrySet()) {
          entry.getValue().complete(entry.getKey());
        }
        futures.clear();
      }
    }

    /**
     * Remote replica.
     */
    private class Replica {
      private final int id;
      private final Member member;
      private final MemberState state;
      private boolean committing;

      private Replica(int id, Member member) {
        this.id = id;
        this.member = member;
        state = context.getCluster().getMember(member.id());
        state.setNextIndex(Math.max(state.getMatchIndex(), Math.max(context.getLog().lastIndex(), 1)));
      }

      /**
       * Triggers a commit for the replica.
       */
      private void commit() {
        if (!committing && isOpen()) {
          // If the log is empty then send an empty commit.
          // If the next index hasn't yet been set then we send an empty commit first.
          // If the next index is greater than the last index then send an empty commit.
          if (context.getLog().isEmpty() || state.getNextIndex() > context.getLog().lastIndex()) {
            emptyCommit();
          } else {
            entriesCommit();
          }
        }
      }

      /**
       * Gets the previous index.
       */
      private long getPrevIndex() {
        return state.getNextIndex() - 1;
      }

      /**
       * Gets the previous entry.
       */
      private RaftEntry getPrevEntry(long prevIndex) {
        if (context.getLog().containsIndex(prevIndex)) {
          return context.getLog().getEntry(prevIndex);
        }
        return null;
      }

      /**
       * Gets a list of entries to send.
       */
      @SuppressWarnings("unchecked")
      private List<RaftEntry> getEntries(long prevIndex) {
        long index;
        if (context.getLog().isEmpty()) {
          return Collections.EMPTY_LIST;
        } else if (prevIndex != 0) {
          index = prevIndex + 1;
        } else {
          index = context.getLog().firstIndex();
        }

        List<RaftEntry> entries = new ArrayList<>(1024);
        int size = 0;
        while (size < MAX_BATCH_SIZE && index <= context.getLog().lastIndex()) {
          RaftEntry entry = context.getLog().getEntry(index);
          if (entry != null) {
            size += entry.size();
            entries.add(entry);
          }
          index++;
        }
        return entries;
      }

      /**
       * Performs an empty commit.
       */
      @SuppressWarnings("unchecked")
      private void emptyCommit() {
        long prevIndex = getPrevIndex();
        RaftEntry prevEntry = getPrevEntry(prevIndex);
        commit(prevIndex, prevEntry, Collections.EMPTY_LIST);
      }

      /**
       * Performs a commit with entries.
       */
      private void entriesCommit() {
        long prevIndex = getPrevIndex();
        RaftEntry prevEntry = getPrevEntry(prevIndex);
        List<RaftEntry> entries = getEntries(prevIndex);
        commit(prevIndex, prevEntry, entries);
      }

      /**
       * Sends a commit message.
       */
      private void commit(long prevIndex, RaftEntry prevEntry, List<RaftEntry> entries) {
        AppendRequest request = AppendRequest.builder()
          .withTerm(context.getTerm())
          .withLeader(context.getMemberId())
          .withLogIndex(prevIndex)
          .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
          .withEntries(entries)
          .withCommitIndex(context.getCommitIndex())
          .withGlobalIndex(context.getGlobalIndex())
          .build();

        committing = true;
        LOGGER.debug("{} - Sent {} to {}", context.getMemberId(), request, this.member);
        context.getConnections().getConnection(member).thenAccept(connection -> {
          connection.<AppendRequest, AppendResponse>send(request).whenComplete((response, error) -> {
            committing = false;
            context.checkThread();

            if (isOpen()) {
              if (error == null) {
                LOGGER.debug("{} - Received {} from {}", context.getMemberId(), response, this.member);
                if (response.status() == Response.Status.OK) {
                  // Update the commit time for the replica. This will cause heartbeat futures to be triggered.
                  commitTime(id);

                  // If replication succeeded then trigger commit futures.
                  if (response.succeeded()) {
                    updateMatchIndex(response);
                    updateNextIndex();

                    // If entries were committed to the replica then check commit indexes.
                    if (!entries.isEmpty()) {
                      commitEntries();
                    }

                    // If there are more entries to send then attempt to send another commit.
                    if (hasMoreEntries()) {
                      commit();
                    }
                  } else if (response.term() > context.getTerm()) {
                    context.setLeader(0);
                    transition(RaftServer.State.FOLLOWER);
                  } else {
                    resetMatchIndex(response);
                    resetNextIndex();

                    // If there are more entries to send then attempt to send another commit.
                    if (hasMoreEntries()) {
                      commit();
                    }
                  }
                } else if (response.term() > context.getTerm()) {
                  LOGGER.debug("{} - Received higher term from {}", context.getMemberId(), this.member);
                  context.setLeader(0);
                  transition(RaftServer.State.FOLLOWER);
                } else {
                  LOGGER.warn("{} - {}", context.getMemberId(), response.error() != null ? response.error() : "");
                }
              } else {
                LOGGER.warn("{} - {}", context.getMemberId(), error.getMessage());

                // Verify that the leader has contacted a majority of the cluster within the last two election timeouts.
                // If the leader is not able to contact a majority of the cluster within two election timeouts, assume
                // that a partition occurred and transition back to the FOLLOWER state.
                if (System.currentTimeMillis() - commitTime() > context.getElectionTimeout() * 2) {
                  LOGGER.warn("{} - Suspected network partition. Stepping down", context.getMemberId());
                  context.setLeader(0);
                  transition(RaftServer.State.FOLLOWER);
                }
              }
            }
            request.close();
          });
        });
      }

      /**
       * Returns a boolean value indicating whether there are more entries to send.
       */
      private boolean hasMoreEntries() {
        return state.getNextIndex() < context.getLog().lastIndex();
      }

      /**
       * Updates the match index when a response is received.
       */
      private void updateMatchIndex(AppendResponse response) {
        // If the replica returned a valid match index then update the existing match index. Because the
        // replicator pipelines replication, we perform a MAX(matchIndex, logIndex) to get the true match index.
        state.setMatchIndex(Math.max(state.getMatchIndex(), response.logIndex()));
      }

      /**
       * Updates the next index when the match index is updated.
       */
      private void updateNextIndex() {
        // If the match index was set, update the next index to be greater than the match index if necessary.
        // Note that because of pipelining append requests, the next index can potentially be much larger than
        // the match index. We rely on the algorithm to reject invalid append requests.
        state.setNextIndex(Math.max(state.getNextIndex(), Math.max(state.getMatchIndex() + 1, 1)));
      }

      /**
       * Resets the match index when a response fails.
       */
      private void resetMatchIndex(AppendResponse response) {
        state.setMatchIndex(response.logIndex());
        LOGGER.debug("{} - Reset match index for {} to {}", context.getMemberId(), member, state.getMatchIndex());
      }

      /**
       * Resets the next index when a response fails.
       */
      private void resetNextIndex() {
        if (state.getMatchIndex() != 0) {
          state.setNextIndex(state.getMatchIndex() + 1);
        } else {
          state.setNextIndex(context.getLog().firstIndex());
        }
        LOGGER.debug("{} - Reset next index for {} to {}", context.getMemberId(), member, state.getNextIndex());
      }

    }
  }

}
