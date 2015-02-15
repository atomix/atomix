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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.raft.protocol.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Leader status.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class LeaderState extends ActiveState {
  private static final int MAX_BATCH_SIZE = 1024 * 1024;
  private ScheduledFuture<?> currentTimer;
  private final Replicator replicator = new Replicator();
  private boolean configuring;

  public LeaderState(RaftContext context) {
    super(context);
  }

  @Override
  public Type type() {
    return Type.LEADER;
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    return super.open()
      .thenRun(this::applyEntries)
      .thenRun(this::commitConfiguration)
      .thenRun(replicator::commit)
      .thenRun(this::takeLeadership)
      .thenRun(this::startHeartbeatTimer);
  }

  /**
   * Commits the current cluster configuration to the log.
   */
  private void commitConfiguration() {
    Set<RaftMember> members = new HashSet<>(context.getMembers().size());
    context.getMembers().stream()
      .filter(m -> m.type() == RaftMember.Type.ACTIVE || m.type() == RaftMember.Type.PROMOTABLE)
      .forEach(members::add);
    commitConfig(members);
  }

  /**
   * Sets the current node as the cluster leader.
   */
  private void takeLeadership() {
    context.setLeader(context.getLocalMember().uri());
  }

  /**
   * Applies all unapplied entries to the log.
   */
  private void applyEntries() {
    Long lastIndex = context.log().lastIndex();
    if (lastIndex != null) {
      int count = 0;
      for (long commitIndex = context.getCommitIndex() != null ? Long.valueOf(context.getCommitIndex() + 1) : context.log().firstIndex(); commitIndex <= lastIndex; commitIndex++) {
        context.setCommitIndex(commitIndex);
        applyEntry(commitIndex);
        count++;
      }
      LOGGER.debug("{} - Applied {} entries to log", context.getLocalMember().uri(), count);
    }
  }

  /**
   * Starts heartbeating all cluster members.
   */
  private void startHeartbeatTimer() {
    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
    LOGGER.debug("{} - Setting heartbeat timer", context.getLocalMember().uri());
    currentTimer = context.executor().scheduleAtFixedRate(this::heartbeatMembers, 0, context.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
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
  public CompletableFuture<JoinResponse> join(final JoinRequest request) {
    logRequest(request);

    if (configuring) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
        .withUri(context.getLocalMember().uri())
        .withTerm(context.getTerm())
        .build()));
    }

    if (!context.getMembers().stream()
      .filter(m -> m.type() == RaftMember.Type.ACTIVE || m.type() == RaftMember.Type.PROMOTABLE)
      .collect(Collectors.toSet()).contains(request.member())) {

      final Set<RaftMember> members = new HashSet<>(context.getMembers().size());
      context.getMembers().stream()
        .filter(m -> m.type() == RaftMember.Type.ACTIVE || m.type() == RaftMember.Type.PROMOTABLE)
        .forEach(members::add);
      final RaftMember member = new RaftMember(request.member(), RaftMember.Type.PROMOTABLE, RaftMember.Status.ALIVE);
      members.add(member);

      CompletableFuture<JoinResponse> future = new CompletableFuture<>();
      commitConfig(members).whenComplete((result, error) -> {
        context.checkThread();
        if (isOpen()) {
          if (error == null) {
            context.addMember(member);
            replicator.update();
            future.complete(logResponse(JoinResponse.builder()
              .withUri(context.getLocalMember().uri())
              .withTerm(context.getTerm())
              .build()));
          } else {
            future.complete(logResponse(JoinResponse.builder()
              .withUri(context.getLocalMember().uri())
              .withStatus(Response.Status.ERROR)
              .withError(error)
              .withTerm(context.getTerm())
              .build()));
          }
        }
      });
      return future;
    } else {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
        .withUri(context.getLocalMember().uri())
        .withTerm(context.getTerm())
        .build()));
    }
  }

  @Override
  public CompletableFuture<PromoteResponse> promote(final PromoteRequest request) {
    logRequest(request);

    if (configuring) {
      return CompletableFuture.completedFuture(logResponse(PromoteResponse.builder()
        .withUri(context.getLocalMember().uri())
        .withTerm(context.getTerm())
        .build()));
    }

    RaftMember member = context.getMember(request.member());
    if (member != null) {
      final Set<RaftMember> members = new HashSet<>(context.getMembers().size());
      context.getMembers().stream()
        .filter(m -> m.type() == RaftMember.Type.ACTIVE || m.type() == RaftMember.Type.PROMOTABLE)
        .forEach(members::add);
      members.remove(member);
      members.add(new RaftMember(member.uri(), RaftMember.Type.ACTIVE, RaftMember.Status.ALIVE));

      CompletableFuture<PromoteResponse> future = new CompletableFuture<>();
      commitConfig(members).whenComplete((result, error) -> {
        if (isOpen()) {
          if (error == null) {
            context.addMember(member);
            replicator.update();
            future.complete(logResponse(PromoteResponse.builder()
              .withUri(context.getLocalMember().uri())
              .withTerm(context.getTerm())
              .withSucceeded(true)
              .build()));
          } else {
            future.complete(logResponse(PromoteResponse.builder()
              .withUri(context.getLocalMember().uri())
              .withStatus(Response.Status.ERROR)
              .withError(error)
              .withTerm(context.getTerm())
              .build()));
          }
        }
      });
      return future;
    } else {
      return CompletableFuture.completedFuture(logResponse(PromoteResponse.builder()
        .withUri(context.getLocalMember().uri())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .build()));
    }
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(final LeaveRequest request) {
    logRequest(request);

    if (configuring) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
        .withUri(context.getLocalMember().uri())
        .withTerm(context.getTerm())
        .build()));
    }

    if (context.getMembers().stream()
      .filter(m -> m.type() == RaftMember.Type.ACTIVE || m.type() == RaftMember.Type.PROMOTABLE)
      .collect(Collectors.toSet()).contains(request.member())) {

      final Set<RaftMember> members = new HashSet<>(context.getMembers().size());
      context.getMembers().stream()
        .filter(m -> !m.uri().equals(request.member()) && (m.type() == RaftMember.Type.ACTIVE || m.type() == RaftMember.Type.PROMOTABLE))
        .forEach(members::add);

      CompletableFuture<LeaveResponse> future = new CompletableFuture<>();
      commitConfig(members).whenComplete((result, error) -> {
        if (isOpen()) {
          if (error == null) {
            RaftMember member = context.getMember(request.member());
            if (member != null) {
              context.removeMember(member);
            }
            replicator.update();

            future.complete(logResponse(LeaveResponse.builder()
              .withUri(context.getLocalMember().uri())
              .withTerm(context.getTerm())
              .build()));
          } else {
            future.complete(logResponse(LeaveResponse.builder()
              .withUri(context.getLocalMember().uri())
              .withStatus(Response.Status.ERROR)
              .withError(error)
              .withTerm(context.getTerm())
              .build()));
          }
        }
      });
      return future;
    } else {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
        .withUri(context.getLocalMember().uri())
        .withTerm(context.getTerm())
        .build()));
    }
  }

  /**
   * Commits a configuration change to the replicated log.
   */
  private CompletableFuture<Long> commitConfig(Set<RaftMember> members) {
    final long term = context.getTerm();
    ByteBuffer config = serializer.writeObject(members);
    ByteBuffer entry = ByteBuffer.allocate(config.limit() + 9);
    entry.putLong(term);
    entry.put(ENTRY_TYPE_CONFIG);
    entry.put(config);
    entry.flip();

    final long index;
    try {
      index = context.log().appendEntry(entry);
      context.log().flush();
    } catch (IOException e) {
      return exceptionalFuture(e);
    }

    LOGGER.debug("{} - Appended entry to log at index {}", context.getLocalMember().uri(), index);
    LOGGER.debug("{} - Replicating logs up to index {} for write", context.getLocalMember().uri(), index);

    configuring = true;
    return replicator.commit(index).whenComplete((result, error) -> {
      configuring = false;
      context.checkThread();
      if (isOpen() && error == null) {
        try {
          context.commitHandler().commit(term, index, null);
        } finally {
          context.setLastApplied(index);
        }
      }
    });
  }

  @Override
  public CompletableFuture<PollResponse> poll(final PollRequest request) {
    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
      .withUri(context.getLocalMember().uri())
      .withTerm(context.getTerm())
      .withAccepted(false)
      .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(final VoteRequest request) {
    if (request.term() > context.getTerm()) {
      LOGGER.debug("{} - Received greater term", context.getLocalMember().uri());
      transition(Type.FOLLOWER);
      return super.vote(request);
    } else {
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
        .withUri(context.getLocalMember().uri())
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
        .withUri(context.getLocalMember().uri())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build()));
    } else {
      transition(Type.FOLLOWER);
      return super.append(request);
    }
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    context.checkThread();
    logRequest(request);

    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    CommitHandler committer = context.commitHandler();

    switch (request.consistency()) {
      // Consistency mode WEAK or DEFAULT is immediately evaluated and returned.
      case WEAK:
      case DEFAULT:
        future.complete(logResponse(QueryResponse.builder()
          .withUri(context.getLocalMember().uri())
          .withResult(committer.commit(context.getTerm(), null, request.entry()))
          .build()));
        break;
      // Consistency mode STRONG requires synchronous consistency check prior to applying the query.
      case STRONG:
        LOGGER.debug("{} - Synchronizing logs to index {} for read", context.getLocalMember().uri(), context.log().lastIndex());
        long term = context.getTerm();
        replicator.commit().whenComplete((index, error) -> {
          context.checkThread();
          if (isOpen()) {
            if (error == null) {
              try {
                future.complete(logResponse(QueryResponse.builder()
                  .withUri(context.getLocalMember().uri())
                  .withResult(committer.commit(term, null, request.entry()))
                  .build()));
              } catch (Exception e) {
                future.complete(logResponse(QueryResponse.builder()
                  .withUri(context.getLocalMember().uri())
                  .withStatus(Response.Status.ERROR)
                  .withError(e)
                  .build()));
              }
            } else {
              future.complete(logResponse(QueryResponse.builder()
                .withUri(context.getLocalMember().uri())
                .withStatus(Response.Status.ERROR)
                .withError(error)
                .build()));
            }
          }
        });
        break;
    }
    return future;
  }

  @Override
  public CompletableFuture<CommandResponse> command(final CommandRequest request) {
    context.checkThread();
    logRequest(request);

    CompletableFuture<CommandResponse> future = new CompletableFuture<>();
    ByteBuffer entry = request.entry();
    CommitHandler committer = context.commitHandler();

    // Create a log entry containing the current term and entry.
    ByteBuffer logEntry = ByteBuffer.allocate(entry.capacity() + 9);
    long term = context.getTerm();
    logEntry.putLong(term);
    logEntry.put(ENTRY_TYPE_USER);
    logEntry.put(entry);
    entry.flip();

    // Try to append the entry to the log. If appending the entry fails then just reply with an exception immediately.
    final long index;
    try {
      index = context.log().appendEntry(logEntry);
      context.log().flush();
    } catch (IOException e) {
      future.completeExceptionally(new CopycatException(e));
      return future;
    }

    LOGGER.debug("{} - Appended entry to log at index {}", context.getLocalMember().uri(), index);
    LOGGER.debug("{} - Replicating logs up to index {} for write", context.getLocalMember().uri(), index);

    // Attempt to replicate the entry to a quorum of the cluster.
    replicator.commit(index).whenComplete((resultIndex, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          try {
            future.complete(logResponse(CommandResponse.builder()
              .withUri(context.getLocalMember().uri())
              .withResult(committer.commit(term, index, entry))
              .build()));
          } catch (Exception e) {
            future.complete(logResponse(CommandResponse.builder()
              .withUri(context.getLocalMember().uri())
              .withStatus(Response.Status.ERROR)
              .withError(e)
              .build()));
          } finally {
            context.setLastApplied(index);
          }
        } else {
          future.complete(logResponse(CommandResponse.builder()
            .withUri(context.getLocalMember().uri())
            .withStatus(Response.Status.ERROR)
            .withError(error)
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
      LOGGER.debug("{} - Cancelling ping timer", context.getLocalMember().uri());
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
    private CompletableFuture<Void> commitFuture;
    private CompletableFuture<Void> nextCommitFuture;
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
      Set<RaftMember> members = context.getMembers().stream()
        .filter(m -> !m.uri().equals(context.getLocalMember().uri()) && m.type() == RaftMember.Type.ACTIVE)
        .collect(Collectors.toSet());
      Set<String> uris = members.stream().map(RaftMember::uri).collect(Collectors.toSet());

      Iterator<Replica> iterator = replicas.iterator();
      while (iterator.hasNext()) {
        Replica replica = iterator.next();
        if (!uris.contains(replica.member.uri())) {
          iterator.remove();
          commitTimes.remove(replica.id);
        }
      }

      Set<String> replicas = this.replicas.stream().map(r -> r.member.uri()).collect(Collectors.toSet());
      for (RaftMember member : members) {
        if (!replicas.contains(member.uri())) {
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
    private CompletableFuture<Void> commit() {
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
      return commitFutures.computeIfAbsent(index, i -> {
        replicas.forEach(Replica::commit);
        return new CompletableFuture<>();
      });
    }

    /**
     * Sets a commit time.
     */
    private void commitTime(int id) {
      commitTimes.set(id, System.currentTimeMillis());

      // Sort the list of commit times. Use the quorum index to get the last time the majority of the cluster
      // was contacted. If the current commitFuture's time is less than the commit time then trigger the
      // commit future and reset it to the next commit future.
      Collections.sort(commitTimes);
      long commitTime = commitTimes.get(quorumIndex);
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
      Collections.sort(replicas, (o1, o2) -> Long.compare(o2.matchIndex != null ? o2.matchIndex : 0L, o1.matchIndex != null ? o1.matchIndex : 0L));

      // Set the current commit index as the median replicated index.
      // Since replicas is a list with zero based indexes, use the negation of
      // the required quorum size to get the index of the replica with the least
      // possible quorum replication. That replica's match index is the commit index.
      // Set the commit index. Once the commit index has been set we can run
      // all tasks up to the given commit.
      Long commitIndex = replicas.get(quorumIndex).matchIndex;
      if (commitIndex != null) {
        context.setCommitIndex(commitIndex);
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
      private final List<ByteBuffer> EMPTY_LIST = new ArrayList<>(0);
      private final int id;
      private final RaftMember member;
      private Long nextIndex;
      private Long matchIndex;
      private boolean committing;

      private Replica(int id, RaftMember member) {
        this.id = id;
        this.member = member;
      }

      /**
       * Triggers a commit for the replica.
       */
      private void commit() {
        if (!committing && isOpen()) {
          // If the log is empty then send an empty commit.
          // If the next index hasn't yet been set then we send an empty commit first.
          // If the next index is greater than the last index then send an empty commit.
          if (context.log().isEmpty() || nextIndex == null || nextIndex > context.log().lastIndex()) {
            emptyCommit();
          } else {
            entriesCommit();
          }
        }
      }

      /**
       * Gets the previous index.
       */
      private Long getPrevIndex() {
        if (nextIndex == null) {
          return context.log().isEmpty() ? null : context.log().lastIndex();
        }
        return nextIndex - 1 > 0 ? nextIndex - 1 : null;
      }

      /**
       * Gets the previous entry.
       */
      private ByteBuffer getPrevEntry(Long prevIndex) {
        if (prevIndex != null && context.log().containsIndex(prevIndex)) {
          return context.log().getEntry(prevIndex);
        }
        return null;
      }

      /**
       * Gets a list of entries to send.
       */
      private List<ByteBuffer> getEntries(Long prevIndex) {
        long index;
        if (context.log().isEmpty()) {
          return EMPTY_LIST;
        } else if (prevIndex != null) {
          index = prevIndex + 1;
        } else {
          index = context.log().firstIndex();
        }

        List<ByteBuffer> entries = new ArrayList<>(1024);
        int size = 0;
        while (size < MAX_BATCH_SIZE && index <= context.log().lastIndex()) {
          ByteBuffer entry = context.log().getEntry(index);
          size += entry.limit();
          entries.add(entry);
          index++;
        }
        return entries;
      }

      /**
       * Performs an empty commit.
       */
      private void emptyCommit() {
        Long prevIndex = getPrevIndex();
        ByteBuffer prevEntry = getPrevEntry(prevIndex);
        commit(prevIndex, prevEntry, EMPTY_LIST);
      }

      /**
       * Performs a commit with entries.
       */
      private void entriesCommit() {
        Long prevIndex = getPrevIndex();
        ByteBuffer prevEntry = getPrevEntry(prevIndex);
        List<ByteBuffer> entries = getEntries(prevIndex);
        commit(prevIndex, prevEntry, entries);
      }

      /**
       * Sends a commit message.
       */
      private void commit(Long prevIndex, ByteBuffer prevEntry, List<ByteBuffer> entries) {
        AppendRequest request = AppendRequest.builder()
          .withUri(member.uri())
          .withTerm(context.getTerm())
          .withLeader(context.getLocalMember().uri())
          .withLogIndex(prevIndex)
          .withLogTerm(prevEntry != null ? prevEntry.getLong() : null)
          .withEntries(entries)
          .withFirstIndex(prevIndex == null || context.log().firstIndex() == prevIndex + 1)
          .withCommitIndex(context.getCommitIndex())
          .build();

        committing = true;
        LOGGER.debug("{} - Sent {} to {}", context.getLocalMember().uri(), request, member);
        appendHandler.apply(request).whenCompleteAsync((response, error) -> {
          committing = false;
          context.checkThread();

          if (isOpen()) {
            if (error == null) {
              LOGGER.debug("{} - Received {} from {}", context.getLocalMember().uri(), response, member);
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
                  transition(Type.FOLLOWER);
                } else {
                  resetMatchIndex(response);
                  resetNextIndex();

                  // If there are more entries to send then attempt to send another commit.
                  if (hasMoreEntries()) {
                    commit();
                  }
                }
              } else if (response.term() > context.getTerm()) {
                LOGGER.debug("{} - Received higher term from {}", context.getLocalMember().uri(), member);
                transition(Type.FOLLOWER);
              } else {
                LOGGER.warn("{} - {}", context.getLocalMember().uri(), response.error() != null ? response.error().getMessage() : "");
              }
            } else {
              LOGGER.warn("{} - {}", context.getLocalMember().uri(), error.getMessage());
            }
          }
        }, context.executor());
      }

      /**
       * Returns a boolean value indicating whether there are more entries to send.
       */
      private boolean hasMoreEntries() {
        return nextIndex != null && !context.log().isEmpty() && nextIndex < context.log().lastIndex();
      }

      /**
       * Updates the match index when a response is received.
       */
      private void updateMatchIndex(AppendResponse response) {
        // If the replica returned a valid match index then update the existing match index. Because the
        // replicator pipelines replication, we perform a MAX(matchIndex, logIndex) to get the true match index.
        if (response.logIndex() != null) {
          if (matchIndex != null) {
            matchIndex = Math.max(matchIndex, response.logIndex());
          } else {
            matchIndex = response.logIndex();
          }
        }
      }

      /**
       * Updates the next index when the match index is updated.
       */
      private void updateNextIndex() {
        // If the match index was set, update the next index to be greater than the match index if necessary.
        // Note that because of pipelining append requests, the next index can potentially be much larger than
        // the match index. We rely on the algorithm to reject invalid append requests.
        if (matchIndex != null) {
          if (nextIndex != null) {
            nextIndex = Math.max(nextIndex, matchIndex + 1);
          } else {
            nextIndex = matchIndex + 1;
          }
        }
      }

      /**
       * Resets the match index when a response fails.
       */
      private void resetMatchIndex(AppendResponse response) {
        if (matchIndex == null) {
          matchIndex = response.logIndex();
        } else if (response.logIndex() != null) {
          matchIndex = Math.max(matchIndex, response.logIndex());
        }
        LOGGER.debug("{} - Reset match index for {} to {}", context.getLocalMember().uri(), member, matchIndex);
      }

      /**
       * Resets the next index when a response fails.
       */
      private void resetNextIndex() {
        if (matchIndex != null) {
          nextIndex = matchIndex + 1;
        } else {
          nextIndex = context.log().firstIndex();
        }
        LOGGER.debug("{} - Reset next index for {} to {}", context.getLocalMember().uri(), member, nextIndex);
      }

    }
  }

}
