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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.raft.ApplicationException;
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.Query;
import net.kuujo.copycat.raft.RaftError;
import net.kuujo.copycat.raft.log.entry.*;
import net.kuujo.copycat.raft.rpc.*;

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
  private static final int MAX_BATCH_SIZE = 1024 * 1024;
  private ScheduledFuture<?> currentTimer;
  private final Replicator replicator = new Replicator();

  public LeaderState(RaftContext context) {
    super(context);
  }

  @Override
  public RaftState type() {
    return RaftState.LEADER;
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
    context.setLeader(context.getCluster().member().id());
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
          transition(RaftState.FOLLOWER);
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
      for (long lastApplied = Math.max(context.getLastApplied(), context.getLog().firstIndex()); lastApplied <= index; lastApplied++) {
        try (Entry entry = context.getLog().getEntry(lastApplied)) {
          if (entry != null) {
            try {
              context.getStateMachine().apply(entry);
            } catch (ApplicationException e) {
              LOGGER.info("{} - an application error occurred: {}", context.getCluster().member().id(), e);
            } finally {
              context.setLastApplied(lastApplied);
            }
          }
        }
        count++;
      }
      LOGGER.debug("{} - applied {} entries to log", context.getCluster().member().id(), count);
    }
  }

  /**
   * Starts heartbeating all cluster members.
   */
  private void startHeartbeatTimer() {
    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
    LOGGER.debug("{} - Setting heartbeat timer", context.getCluster().member().id());
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
      LOGGER.debug("{} - Received greater term", context.getCluster().member().id());
      transition(RaftState.FOLLOWER);
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
      transition(RaftState.FOLLOWER);
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
      LOGGER.debug("{} - Appended entry to log at index {}", context.getCluster().member().id(), index);
    }

    CompletableFuture<CommandResponse> future = new CompletableFuture<>();
    replicator.commit(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          try (CommandEntry entry = context.getLog().getEntry(index)) {
            try {
              future.complete(logResponse(CommandResponse.builder()
                .withStatus(Response.Status.OK)
                .withResult(context.getStateMachine().apply(entry))
                .build()));
            } catch (ApplicationException e) {
              future.complete(logResponse(CommandResponse.builder()
                .withStatus(Response.Status.ERROR)
                .withError(RaftError.Type.APPLICATION_ERROR)
                .build()));
            } catch (Exception e) {
              future.completeExceptionally(e);
            } finally {
              context.setLastApplied(commitIndex);
            }
          }
        } else {
          future.complete(logResponse(CommandResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.COMMAND_ERROR)
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

    QueryEntry entry = new QueryEntry(index)
      .setTerm(context.getTerm())
      .setSession(request.session())
      .setTimestamp(timestamp)
      .setQuery(query);

    Query.Consistency consistency = query.consistency();
    if (consistency == null)
      return submitQueryLinearizableStrict(entry);

    switch (consistency) {
      case SERIALIZABLE:
        return submitQuerySerializable(entry);
      case LINEARIZABLE_LEASE:
        return submitQueryLinearizableLease(entry);
      case LINEARIZABLE_STRICT:
        return submitQueryLinearizableStrict(entry);
      default:
        throw new IllegalStateException("unknown consistency level");
    }
  }

  /**
   * Submits a query with serializable consistency.
   */
  private CompletableFuture<QueryResponse> submitQuerySerializable(OperationEntry entry) {
    return applyQuery(entry, new CompletableFuture<>());
  }

  /**
   * Submits a query with lease based linearizable consistency.
   */
  private CompletableFuture<QueryResponse> submitQueryLinearizableLease(OperationEntry entry) {
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
  private CompletableFuture<QueryResponse> submitQueryLinearizableStrict(OperationEntry entry) {
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
    });
    return future;
  }

  /**
   * Applies a query to the state machine.
   */
  private CompletableFuture<QueryResponse> applyQuery(OperationEntry entry, CompletableFuture<QueryResponse> future) {
    try {
      future.complete(logResponse(QueryResponse.builder()
        .withStatus(Response.Status.OK)
        .withResult(context.getStateMachine().apply(entry))
        .build()));
    } catch (ApplicationException e) {
      future.complete(logResponse(QueryResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.APPLICATION_ERROR)
        .build()));
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
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
      entry.setMember(request.member());
      entry.setTimestamp(timestamp);
      index = context.getLog().appendEntry(entry);
      LOGGER.debug("{} - Appended register entry to log at index {}", context.getCluster().member().id(), index);
    }

    CompletableFuture<RegisterResponse> future = new CompletableFuture<>();
    replicator.commit(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          try (RegisterEntry entry = context.getLog().getEntry(index)) {
            try {
              future.complete(logResponse(RegisterResponse.builder()
                .withStatus(Response.Status.OK)
                .withLeader(context.getLeader())
                .withTerm(context.getTerm())
                .withSession(context.getStateMachine().apply(entry))
                .withMembers(context.getCluster().members().stream().map(Member::info).collect(Collectors.toList()))
                .build()));
            } catch (ApplicationException e) {
              future.complete(logResponse(RegisterResponse.builder()
                .withStatus(Response.Status.ERROR)
                .withError(RaftError.Type.APPLICATION_ERROR)
                .build()));
            } catch (Exception e) {
              future.completeExceptionally(e);
            } finally {
              context.setLastApplied(commitIndex);
            }
          }
        } else {
          future.complete(logResponse(RegisterResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.COMMAND_ERROR)
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
      LOGGER.debug("{} - Appended session entry to log at index {}", context.getCluster().member().id(), index);
    }

    CompletableFuture<KeepAliveResponse> future = new CompletableFuture<>();
    replicator.commit(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          try (KeepAliveEntry entry = context.getLog().getEntry(index)) {
            try {
              context.getStateMachine().apply(entry);
              future.complete(logResponse(KeepAliveResponse.builder()
                .withStatus(Response.Status.OK)
                .withLeader(context.getLeader())
                .withTerm(context.getTerm())
                .withMembers(context.getCluster().members().stream().map(Member::info).collect(Collectors.toList()))
                .build()));
            } catch (ApplicationException e) {
              future.complete(logResponse(KeepAliveResponse.builder()
                .withStatus(Response.Status.ERROR)
                .withError(RaftError.Type.APPLICATION_ERROR)
                .build()));
            } catch (Exception e) {
              future.completeExceptionally(e);
            } finally {
              context.setLastApplied(commitIndex);
            }
          }
        } else {
          future.complete(logResponse(KeepAliveResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.COMMAND_ERROR)
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
      LOGGER.debug("{} - Cancelling ping timer", context.getCluster().member().id());
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
      update();
    }

    /**
     * Updates the replicator's cluster configuration.
     */
    private void update() {
      Set<Member> members = context.getCluster().members().stream()
        .filter(m -> m.id() != context.getCluster().member().id() && m.type() == Member.Type.ACTIVE)
        .collect(Collectors.toSet());
      Set<Integer> ids = members.stream().map(Member::id).collect(Collectors.toSet());

      Iterator<Replica> iterator = replicas.iterator();
      while (iterator.hasNext()) {
        Replica replica = iterator.next();
        if (!ids.contains(replica.member.id())) {
          iterator.remove();
          commitTimes.remove(replica.id);
        }
      }

      Set<Integer> replicas = this.replicas.stream().map(r -> r.member.id()).collect(Collectors.toSet());
      for (Member member : members) {
        if (!replicas.contains(member.id())) {
          this.replicas.add(new Replica(this.replicas.size(), member));
          this.commitTimes.add(System.currentTimeMillis());
        }
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
      Collections.sort(commitTimes);
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
      if (commitFuture != null && this.commitTime >= commitTime) {
        commitFuture.complete(null);
        commitFuture = nextCommitFuture;
        nextCommitFuture = null;
        if (this.commitFuture != null) {
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
      Collections.sort(replicas, (o1, o2) -> Long.compare(o2.matchIndex != 0 ? o2.matchIndex : 0l, o1.matchIndex != 0 ? o1.matchIndex : 0l));

      // Set the current commit index as the median replicated index.
      // Since replicas is a list with zero based indexes, use the negation of
      // the required quorum size to get the index of the replica with the least
      // possible quorum replication. That replica's match index is the commit index.
      // Set the commit index. Once the commit index has been set we can run
      // all tasks up to the given commit.
      long commitIndex = replicas.get(quorumIndex).matchIndex;
      long globalIndex = replicas.get(0).matchIndex;
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
      private long nextIndex = 1;
      private long matchIndex = 0;
      private boolean committing;

      private Replica(int id, Member member) {
        this.id = id;
        this.member = member;
        this.nextIndex = Math.max(context.getLog().lastIndex(), 1);
      }

      /**
       * Triggers a commit for the replica.
       */
      private void commit() {
        if (!committing && isOpen()) {
          // If the log is empty then send an empty commit.
          // If the next index hasn't yet been set then we send an empty commit first.
          // If the next index is greater than the last index then send an empty commit.
          if (context.getLog().isEmpty() || nextIndex > context.getLog().lastIndex()) {
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
        return nextIndex - 1;
      }

      /**
       * Gets the previous entry.
       */
      private Entry getPrevEntry(long prevIndex) {
        if (context.getLog().containsIndex(prevIndex)) {
          return context.getLog().getEntry(prevIndex);
        }
        return null;
      }

      /**
       * Gets a list of entries to send.
       */
      @SuppressWarnings("unchecked")
      private List<Entry> getEntries(long prevIndex) {
        long index;
        if (context.getLog().isEmpty()) {
          return Collections.EMPTY_LIST;
        } else if (prevIndex != 0) {
          index = prevIndex + 1;
        } else {
          index = context.getLog().firstIndex();
        }

        List<Entry> entries = new ArrayList<>(1024);
        int size = 0;
        while (size < MAX_BATCH_SIZE && index <= context.getLog().lastIndex()) {
          Entry entry = context.getLog().getEntry(index);
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
        Entry prevEntry = getPrevEntry(prevIndex);
        commit(prevIndex, prevEntry, Collections.EMPTY_LIST);
      }

      /**
       * Performs a commit with entries.
       */
      private void entriesCommit() {
        long prevIndex = getPrevIndex();
        Entry prevEntry = getPrevEntry(prevIndex);
        List<Entry> entries = getEntries(prevIndex);
        commit(prevIndex, prevEntry, entries);
      }

      /**
       * Sends a commit message.
       */
      private void commit(long prevIndex, Entry prevEntry, List<Entry> entries) {
        AppendRequest request = AppendRequest.builder()
          .withTerm(context.getTerm())
          .withLeader(context.getCluster().member().id())
          .withLogIndex(prevIndex)
          .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
          .withEntries(entries)
          .withCommitIndex(context.getCommitIndex())
          .withGlobalIndex(context.getGlobalIndex())
          .build();

        committing = true;
        LOGGER.debug("{} - Sent {} to {}", context.getCluster().member().id(), request, member);
        member.<AppendRequest, AppendResponse>send(context.getTopic(), request).whenCompleteAsync((response, error) -> {
          committing = false;
          context.checkThread();

          if (isOpen()) {
            if (error == null) {
              LOGGER.debug("{} - Received {} from {}", context.getCluster().member().id(), response, member);
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
                  transition(RaftState.FOLLOWER);
                } else {
                  resetMatchIndex(response);
                  resetNextIndex();

                  // If there are more entries to send then attempt to send another commit.
                  if (hasMoreEntries()) {
                    commit();
                  }
                }
              } else if (response.term() > context.getTerm()) {
                LOGGER.debug("{} - Received higher term from {}", context.getCluster().member().id(), member);
                transition(RaftState.FOLLOWER);
              } else {
                LOGGER.warn("{} - {}", context.getCluster()
                  .member()
                  .id(), response.error() != null ? response.error() : "");
              }
            } else {
              LOGGER.warn("{} - {}", context.getCluster().member().id(), error.getMessage());
            }
          }
          request.release();
        }, context.getContext());
      }

      /**
       * Returns a boolean value indicating whether there are more entries to send.
       */
      private boolean hasMoreEntries() {
        return nextIndex < context.getLog().lastIndex();
      }

      /**
       * Updates the match index when a response is received.
       */
      private void updateMatchIndex(AppendResponse response) {
        // If the replica returned a valid match index then update the existing match index. Because the
        // replicator pipelines replication, we perform a MAX(matchIndex, logIndex) to get the true match index.
        matchIndex = Math.max(matchIndex, response.logIndex());
      }

      /**
       * Updates the next index when the match index is updated.
       */
      private void updateNextIndex() {
        // If the match index was set, update the next index to be greater than the match index if necessary.
        // Note that because of pipelining append requests, the next index can potentially be much larger than
        // the match index. We rely on the algorithm to reject invalid append requests.
        nextIndex = Math.max(nextIndex, Math.max(matchIndex + 1, 1));
      }

      /**
       * Resets the match index when a response fails.
       */
      private void resetMatchIndex(AppendResponse response) {
        if (matchIndex == 0) {
          matchIndex = response.logIndex();
        } else if (response.logIndex() != 0) {
          matchIndex = Math.max(matchIndex, response.logIndex());
        }
        LOGGER.debug("{} - Reset match index for {} to {}", context.getCluster().member().id(), member, matchIndex);
      }

      /**
       * Resets the next index when a response fails.
       */
      private void resetNextIndex() {
        if (matchIndex != 0) {
          nextIndex = matchIndex + 1;
        } else {
          nextIndex = context.getLog().firstIndex();
        }
        LOGGER.debug("{} - Reset next index for {} to {}", context.getCluster().member().id(), member, nextIndex);
      }

    }
  }

}
