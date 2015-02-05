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
package net.kuujo.copycat.resource.internal;

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.protocol.rpc.*;
import net.kuujo.copycat.util.internal.Quorum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Leader state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class LeaderState extends ActiveState {
  private static final int MAX_BATCH_SIZE = 1024 * 1024;
  private ScheduledFuture<?> currentTimer;
  private final Replicator replicator = new Replicator();

  LeaderState(CopycatStateContext context) {
    super(context);
  }

  @Override
  public CopycatState state() {
    return CopycatState.LEADER;
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    return super.open()
      .thenRun(replicator::commit)
      .thenRun(this::takeLeadership)
      .thenRun(this::applyEntries)
      .thenRun(this::startHeartbeatTimer);
  }

  /**
   * Sets the current node as the cluster leader.
   */
  private void takeLeadership() {
    context.setLeader(context.getLocalMember());
  }

  /**
   * Applies all unapplied entries to the log.
   */
  private void applyEntries() {
    Long lastIndex = context.log().lastIndex();
    if (lastIndex != null) {
      int count = 0;
      for (long i = context.getLastApplied() + 1; i <= lastIndex; i++) {
        applyEntry(i);
        count++;
      }
      LOGGER.debug("{} - Applied {} entries to log", context.getLocalMember(), count);
    }
  }

  /**
   * Starts heartbeating all cluster members.
   */
  private void startHeartbeatTimer() {
    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
    LOGGER.debug("{} - Setting heartbeat timer", context.getLocalMember());
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
  public CompletableFuture<PollResponse> poll(final PollRequest request) {
    if (request.term() > context.getTerm()) {
      transition(CopycatState.FOLLOWER);
      return super.poll(request);
    } else {
      return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
        .withUri(context.getLocalMember())
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
        .withUri(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build()));
    } else {
      transition(CopycatState.FOLLOWER);
      return super.append(request);
    }
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    context.checkThread();
    logRequest(request);

    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    BiFunction<Long, ByteBuffer, ByteBuffer> consumer = context.consumer();

    switch (request.consistency()) {
      // Consistency mode WEAK or DEFAULT is immediately evaluated and returned.
      case WEAK:
      case DEFAULT:
        future.complete(logResponse(QueryResponse.builder()
          .withUri(context.getLocalMember())
          .withResult(consumer.apply(null, request.entry()))
          .build()));
        break;
      // Consistency mode STRONG requires synchronous consistency check prior to applying the query.
      case STRONG:
        LOGGER.debug("{} - Synchronizing logs to index {} for read", context.getLocalMember(), context.log().lastIndex());
        replicator.commit().whenComplete((index, error) -> {
          context.checkThread();
          if (isOpen()) {
            if (error == null) {
              try {
                future.complete(logResponse(QueryResponse.builder()
                  .withUri(context.getLocalMember())
                  .withResult(consumer.apply(null, request.entry()))
                  .build()));
              } catch (Exception e) {
                future.complete(logResponse(QueryResponse.builder()
                  .withUri(context.getLocalMember())
                  .withStatus(Response.Status.ERROR)
                  .withError(e)
                  .build()));
              }
            } else {
              future.complete(logResponse(QueryResponse.builder()
                .withUri(context.getLocalMember())
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
  public CompletableFuture<CommitResponse> commit(final CommitRequest request) {
    context.checkThread();
    logRequest(request);

    CompletableFuture<CommitResponse> future = new CompletableFuture<>();
    ByteBuffer entry = request.entry();
    BiFunction<Long, ByteBuffer, ByteBuffer> consumer = context.consumer();

    // Create a log entry containing the current term and entry.
    ByteBuffer logEntry = ByteBuffer.allocate(entry.capacity() + 8);
    logEntry.putLong(context.getTerm());
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

    LOGGER.debug("{} - Appended entry to log at index {}", context.getLocalMember(), index);
    LOGGER.debug("{} - Replicating logs up to index {} for write", context.getLocalMember(), index);

    // Attempt to replicate the entry to a quorum of the cluster.
    replicator.commit(index).whenComplete((resultIndex, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          try {
            future.complete(logResponse(CommitResponse.builder()
              .withUri(context.getLocalMember())
              .withResult(consumer.apply(index, entry))
              .build()));
          } catch (Exception e) {
            future.complete(logResponse(CommitResponse.builder()
              .withUri(context.getLocalMember())
              .withStatus(Response.Status.ERROR)
              .withError(e)
              .build()));
          } finally {
            context.setLastApplied(index);
          }
        } else {
          future.complete(logResponse(CommitResponse.builder()
            .withUri(context.getLocalMember())
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
      LOGGER.debug("{} - Cancelling ping timer", context.getLocalMember());
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
    private final Map<String, Replica> replicaMap;
    private final List<Replica> replicas;
    private int quorum;
    private int quorumIndex;
    private final TreeMap<Long, CompletableFuture<Long>> commitFutures = new TreeMap<>();

    private Replicator() {
      this.replicaMap = new HashMap<>(context.getActiveMembers().size());
      this.replicas = new ArrayList<>(context.getActiveMembers().size());
      for (String uri : context.getActiveMembers()) {
        if (!uri.equals(context.getLocalMember())) {
          Replica replica = new Replica(uri, context);
          replicaMap.put(uri, replica);
          replicas.add(replica);
        }
      }

      // Quorum is floor(replicas.size / 2) since this node is implicitly counted in the quorum count.
      this.quorum = (int) Math.floor(context.getActiveMembers().size() / 2);
      this.quorumIndex = quorum - 1;
    }

    /**
     * Registers a future to be completed on the next heartbeat.
     */
    public CompletableFuture<Long> commit() {
      CompletableFuture<Long> future = new CompletableFuture<>();
      Quorum quorum = new Quorum(this.quorum, succeeded -> {
        if (succeeded) {
          future.complete(context.getCommitIndex());
        } else {
          future.completeExceptionally(new IllegalStateException("Failed to heartbeat cluster"));
        }
      });

      for (Replica replica : replicas) {
        replica.commit().whenComplete((result, error) -> {
          context.checkThread();
          if (error == null) {
            quorum.succeed();
          } else {
            quorum.fail();
          }
        });
      }
      return future;
    }
    
    /**
     * Commits the log up to the given index.
     */
    public CompletableFuture<Long> commit(Long index) {
      context.checkThread();

      CompletableFuture<Long> future = new CompletableFuture<>();
      commitFutures.put(index, future);

      // Iterate through replicas and commit all entries up to the given index.
      for (Replica replica : replicaMap.values()) {
        replica.commit(index).whenComplete((resultIndex, error) -> {
          context.checkThread();
          // Once the commit succeeds, check the commit index of all replicas.
          if (error == null) {
            checkCommits();
          }
        });
      }
      return future;
    }

    /**
     * Determines which message have been committed.
     */
    private void checkCommits() {
      context.checkThread();
      if (!replicas.isEmpty() && quorumIndex >= 0) {
        // Sort the list of replicas, order by the last index that was replicated
        // to the replica. This will allow us to determine the median index
        // for all known replicated entries across all cluster members.
        Collections.sort(replicas, (o1, o2) -> Long.compare(o1.matchIndex != null ? o1.matchIndex : 0, o2.matchIndex != null ? o2.matchIndex : 0));

        // Set the current commit index as the median replicated index.
        // Since replicas is a list with zero based indexes, use the negation of
        // the required quorum size to get the index of the replica with the least
        // possible quorum replication. That replica's match index is the commit index.
        // Set the commit index. Once the commit index has been set we can run
        // all tasks up to the given commit.
        Long commitIndex = replicas.get(quorumIndex).matchIndex;
        if (commitIndex != null) {
          context.setCommitIndex(commitIndex);
          triggerFutures(commitIndex);
        }
      }
    }

    /**
     * Triggers commit futures up to the given index.
     */
    private void triggerFutures(long index) {
      Iterator<Map.Entry<Long, CompletableFuture<Long>>> iterator = commitFutures.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<Long, CompletableFuture<Long>> entry = iterator.next();
        if (entry.getKey() <= index) {
          iterator.remove();
          entry.getValue().complete(entry.getKey());
        } else {
          break;
        }
      }
    }
  }

  /**
   * Remote replica.
   */
  private class Replica {
    private final String member;
    private final CopycatStateContext context;
    private Long nextIndex;
    private Long matchIndex;
    private CompletableFuture<Long> commitFuture;
    private CompletableFuture<Long> nextCommitFuture;
    private final TreeMap<Long, CompletableFuture<Long>> commitFutures = new TreeMap<>();
    private boolean committing;

    private Replica(String member, CopycatStateContext context) {
      this.member = member;
      this.context = context;
      this.nextIndex = !context.log().isEmpty() ? context.log().lastIndex() + 1 : null;
    }

    /**
     * Adds a commit handler for the next request.
     */
    public CompletableFuture<Long> commit() {
      // If a commit is already in progress, queue the next commit future and return it.
      if (committing) {
        if (nextCommitFuture == null) {
          nextCommitFuture = new CompletableFuture<>();
        }
        return nextCommitFuture;
      } else {
        // If no commit is currently in progress, set the current commit future and force the commit.
        if (commitFuture == null) {
          commitFuture = new CompletableFuture<>();
        }
        doCommit();
        return commitFuture;
      }
    }

    /**
     * Commits the given index to the replica.
     */
    public CompletableFuture<Long> commit(long index) {
      if (matchIndex != null && index <= matchIndex) {
        return CompletableFuture.completedFuture(index);
      }

      // If a future exists for an entry with a greater index then return that future.
      Map.Entry<Long, CompletableFuture<Long>> entry = commitFutures.ceilingEntry(index);
      if (entry != null) {
        return entry.getValue();
      }

      CompletableFuture<Long> future = new CompletableFuture<>();
      commitFutures.put(index, future);

      doCommit();
      return future;
    }

    /**
     * Performs a commit operation.
     */
    private void doCommit() {
      if (!committing && (commitFuture != null || !context.log().isEmpty())) {
        if (nextIndex == null) {
          nextIndex = context.log().lastIndex();
        }

        Long prevIndex;
        ByteBuffer prevEntry;
        List<ByteBuffer> entries;
        if (nextIndex == null) {
          prevIndex = null;
          prevEntry = null;
          entries = new ArrayList<>(0);
        } else {
          prevIndex = nextIndex - 1 == 0 ? null : nextIndex - 1;
          prevEntry = prevIndex != null ? context.log().getEntry(prevIndex) : null;
          entries = new ArrayList<>((int) Math.min((context.log().lastIndex() - nextIndex + 1) * 128, MAX_BATCH_SIZE));

          long index = nextIndex;
          int size = 0;
          while (size < MAX_BATCH_SIZE && index <= context.log().lastIndex()) {
            ByteBuffer entry = context.log().getEntry(index);
            size += entry.limit();
            entries.add(entry);
            index++;
          }
        }
        committing = true;
        doCommit(prevIndex, prevEntry, entries);
      }
    }

    /**
     * Sends a append request.
     */
    private void doCommit(final Long prevIndex, final ByteBuffer prevEntry, final List<ByteBuffer> entries) {
      AppendRequest request = AppendRequest.builder()
        .withUri(member)
        .withTerm(context.getTerm())
        .withLeader(context.getLocalMember())
        .withLogIndex(prevIndex)
        .withLogTerm(prevEntry != null ? prevEntry.getLong() : null)
        .withEntries(entries)
        .withFirstIndex(prevIndex == null || context.log().firstIndex() == prevIndex + 1)
        .withCommitIndex(context.getCommitIndex())
        .build();

      LOGGER.debug("{} - Sent {} to {}", context.getLocalMember(), request, member);
      appendHandler.apply(request).whenCompleteAsync((response, error) -> {
        context.checkThread();
        committing = false;
        if (isOpen()) {
          if (error != null) {
            triggerCommitFutures(prevIndex != null ? prevIndex + 1 : context.log().firstIndex(),
              prevIndex != null ? prevIndex + entries.size() : context.log().firstIndex() + entries.size() - 1, error);
          } else {
            LOGGER.debug("{} - Received {} from {}", context.getLocalMember(), response, member);
            if (response.status().equals(Response.Status.OK)) {
              if (response.succeeded()) {
                // Update the next index to send and the last index known to be replicated.
                if (!entries.isEmpty()) {
                  matchIndex = matchIndex != null ? Math.max(matchIndex,
                    prevIndex != null ? prevIndex + entries.size() : context.log().firstIndex() + entries.size() - 1)
                    : prevIndex != null ? prevIndex + entries.size() : context.log().firstIndex() + entries.size() - 1;
                  nextIndex = matchIndex + 1;
                  triggerCommitFutures(prevIndex != null ? prevIndex + 1 : context.log().firstIndex(), matchIndex);
                  doCommit();
                }
              } else {
                if (response.term() > context.getTerm()) {
                  triggerCommitFutures(prevIndex != null ? prevIndex + 1 : context.log().firstIndex(),
                    prevIndex != null ? prevIndex + entries.size() : context.log().firstIndex() + entries.size() - 1,
                    new CopycatException("Not the leader"));
                  transition(CopycatState.FOLLOWER);
                } else {
                  // If replication failed then use the last log index indicated by
                  // the replica in the response to generate a new nextIndex. This allows
                  // us to skip repeatedly replicating one entry at a time if it's not
                  // necessary.
                  nextIndex = response.logIndex() != null ? response.logIndex() + 1
                    : prevIndex != null ? prevIndex : context.log().firstIndex();
                  doCommit();
                }
              }
            } else {
              triggerCommitFutures(prevIndex != null ? prevIndex + 1 : context.log().firstIndex(),
                prevIndex != null ? prevIndex + entries.size() : context.log().firstIndex() + entries.size() - 1,
                response.error());
              doCommit();
            }
          }
        }
      }, context.executor());
    }

    /**
     * Triggers replicate futures with an error result.
     */
    private void triggerCommitFutures(Long startIndex, Long endIndex, Throwable t) {
      if (commitFuture != null) {
        commitFuture.completeExceptionally(t);
      }
      commitFuture = nextCommitFuture != null ? nextCommitFuture : commitFuture;
      nextCommitFuture = null;
      if (endIndex >= startIndex) {
        for (long i = startIndex; i <= endIndex; i++) {
          CompletableFuture<Long> future = commitFutures.remove(i);
          if (future != null) {
            future.completeExceptionally(t);
          }
        }
      }
    }

    /**
     * Triggers replicate futures with a completion result
     */
    private void triggerCommitFutures(Long startIndex, Long endIndex) {
      if (commitFuture != null) {
        commitFuture.complete(endIndex);
      }
      commitFuture = nextCommitFuture != null ? nextCommitFuture : commitFuture;
      nextCommitFuture = null;
      if (endIndex >= startIndex) {
        for (long i = startIndex; i <= endIndex; i++) {
          CompletableFuture<Long> future = commitFutures.remove(i);
          if (future != null) {
            future.complete(i);
          }
        }
      }
    }
  }

}
