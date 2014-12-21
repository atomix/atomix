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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.Managed;
import net.kuujo.copycat.internal.util.Quorum;
import net.kuujo.copycat.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderState.class);
  private static final long CONSISTENCY_LEASE_TIME = 1000;
  private ScheduledFuture<?> currentTimer;
  private Replicator replicator;
  private long lastConsistencyCheck;

  LeaderState(DefaultCopycatStateContext context) {
    super(context);
    this.replicator = new Replicator(context);
  }

  @Override
  public CopycatState state() {
    return CopycatState.LEADER;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public CompletableFuture<Void> open() {
    return super.open()
      .thenRun(this::takeLeadership)
      .thenRun(this::applyEntries)
      .thenRun(this::startPingTimer);
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
   * Starts pinging all cluster members.
   */
  private void startPingTimer() {
    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
    replicator.pingAll();

    LOGGER.debug("{} - Setting ping timer", context.getLocalMember());
    setPingTimer();
  }

  /**
   * Sets the ping timer.
   */
  private void setPingTimer() {
    currentTimer = context.executor().schedule(() -> {
      replicator.pingAll();
      setPingTimer();
    }, context.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
  }

  @Override
  public CompletableFuture<PingResponse> ping(final PingRequest request) {
    if (request.term() > context.getTerm()) {
      return super.ping(request);
    } else if (request.term() < context.getTerm()) {
      return CompletableFuture.completedFuture(logResponse(PingResponse.builder()
        .withId(logRequest(request).id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .build()));
    } else {
      transition(CopycatState.FOLLOWER);
      return super.ping(request);
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(final AppendRequest request) {
    if (request.term() > context.getTerm()) {
      return super.append(request);
    } else if (request.term() < context.getTerm()) {
      return CompletableFuture.completedFuture(logResponse(AppendResponse.builder()
        .withId(logRequest(request).id())
        .withMember(context.getLocalMember())
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
    logRequest(request);

    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    BiFunction<Long, ByteBuffer, ByteBuffer> consumer = context.consumer();

    switch (request.consistency()) {
      // Consistency mode NONE or DEFAULT is immediately evaluated and returned.
      case NONE:
      case DEFAULT:
        future.complete(logResponse(QueryResponse.builder()
          .withId(request.id())
          .withMember(context.getLocalMember())
          .withResult(consumer != null ? consumer.apply(null, request.entry()) : null)
          .build()));
        break;
      // Consistency mode FULL requires synchronous consistency check prior to applying the query.
      case FULL:
        LOGGER.debug("{} - Synchronizing logs to index {} for read", context.getLocalMember(), context.log().lastIndex());
        replicator.pingAll().whenComplete((index, error) -> {
          if (error == null) {
            try {
              future.complete(logResponse(QueryResponse.builder()
                .withId(request.id())
                .withMember(context.getLocalMember())
                .withResult(consumer != null ? consumer.apply(null, request.entry()) : null)
                .build()));
            } catch (Exception e) {
              future.complete(QueryResponse.builder()
                .withId(request.id())
                .withMember(context.getLocalMember())
                .withStatus(Response.Status.ERROR)
                .withError(e)
                .build());
            }
          } else {
            future.complete(QueryResponse.builder()
              .withId(request.id())
              .withMember(context.getLocalMember())
              .withStatus(Response.Status.ERROR)
              .withError(error)
              .build());
          }
        });
        break;
    }
    return future;
  }

  @Override
  public CompletableFuture<CommitResponse> commit(final CommitRequest request) {
    logRequest(request);

    CompletableFuture<CommitResponse> future = new CompletableFuture<>();
    ByteBuffer entry = request.entry();
    BiFunction<Long, ByteBuffer, ByteBuffer> consumer = context.consumer();
    ByteBuffer logEntry = ByteBuffer.allocate(entry.capacity() + 8);
    logEntry.putLong(context.getTerm());
    logEntry.put(entry);
    entry.rewind();
    long index = context.log().appendEntry(logEntry);
    LOGGER.debug("{} - Appended entry to log at index {}", context.getLocalMember(), index);
    LOGGER.debug("{} - Replicating logs up to index {} for write", context.getLocalMember(), index);
    replicator.sync(index).whenComplete((resultIndex, error) -> {
      if (error == null) {
        try {
          future.complete(logResponse(CommitResponse.builder()
            .withId(request.id())
            .withMember(context.getLocalMember())
            .withResult(consumer != null ? consumer.apply(index, entry) : null)
            .build()));
        } catch (Exception e) {
          future.complete(CommitResponse.builder()
            .withId(request.id())
            .withMember(context.getLocalMember())
            .withStatus(Response.Status.ERROR)
            .withError(e)
            .build());
        } finally {
          context.setLastApplied(index);
        }
      } else {
        future.complete(CommitResponse.builder()
          .withId(request.id())
          .withMember(context.getLocalMember())
          .withStatus(Response.Status.ERROR)
          .withError(error)
          .build());
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
      currentTimer.cancel(true);
    }
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRunAsync(this::cancelPingTimer, context.executor());
  }

  /**
   * Log replicator.
   */
  private class Replicator implements Managed {
    private final DefaultCopycatStateContext context;
    private final Map<String, Replica> replicaMap;
    private final List<Replica> replicas;
    private int readQuorum;
    private int writeQuorum;
    private int quorumIndex;
    private final TreeMap<Long, CompletableFuture<Long>> commitFutures = new TreeMap<>();

    private Replicator(DefaultCopycatStateContext context) {
      this.context = context;
      this.replicaMap = new HashMap<>(context.getRemoteMembers().size());
      this.replicas = new ArrayList<>(context.getRemoteMembers().size());
      context.getRemoteMembers().forEach(member -> {
        Replica replica = new Replica(member, context);
        replicaMap.put(member, replica);
        replicas.add(replica);
      });
      this.readQuorum = (int) (Math.floor(context.getMembers().size() / 2) + 1);
      this.writeQuorum = (int) (Math.floor(context.getMembers().size() / 2) + 1);
      this.quorumIndex = writeQuorum - 1;
    }

    /**
     * Pings all nodes in the cluster.
     */
    public CompletableFuture<Long> pingAll() {
      return ping(context.log().lastIndex());
    }

    /**
     * Pings the log using the given index for the consistency check.
     */
    public CompletableFuture<Long> ping(Long index) {
      CompletableFuture<Long> future = new CompletableFuture<>();

      // Set up a read quorum. Once the required number of replicas have been
      // contacted the quorum will succeed.
      final Quorum quorum = new Quorum(readQuorum, succeeded -> {
        if (succeeded) {
          future.complete(index);
        } else {
          future.completeExceptionally(new CopycatException("Failed to obtain quorum"));
        }
      }).countSelf();

      // Iterate through replicas and ping each replica. Internally, this
      // should cause the replica to send any remaining entries if necessary.
      for (Replica replica : replicaMap.values()) {
        replica.ping(index).whenComplete((resultIndex, error) -> {
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
     * Syncs the log to all nodes in the cluster.
     */
    public CompletableFuture<Long> syncAll() {
      return sync(context.log().lastIndex());
    }

    /**
     * Syncs the log up to the given index.
     */
    public CompletableFuture<Long> sync(Long index) {
      if (index == null) {
        return ping(null);
      }

      CompletableFuture<Long> future = new CompletableFuture<>();
      commitFutures.put(index, future);

      // Set up a write quorum. Once the log entry has been replicated to
      // the required number of replicas in order to meet the write quorum
      // requirement, the future will succeed.
      final Quorum quorum = new Quorum(writeQuorum, succeeded -> {
        if (succeeded) {
          future.complete(index);
        } else {
          future.completeExceptionally(new CopycatException("Failed to obtain quorum"));
        }
      }).countSelf();

      // Iterate through replicas and commit all entries up to the given index.
      for (Replica replica : replicaMap.values()) {
        replica.sync(index).whenComplete((resultIndex, error) -> {
          // Once the commit succeeds, check the commit index of all replicas.
          if (error == null) {
            quorum.succeed();
            checkCommits();
          } else {
            quorum.fail();
          }
        });
      }
      return future;
    }

    /**
     * Determines which message have been committed.
     */
    private void checkCommits() {
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
          triggerCommitFutures(commitIndex);
        }
      }
    }

    /**
     * Triggers commit futures up to the given index.
     */
    private synchronized void triggerCommitFutures(long index) {
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

    @Override
    public CompletableFuture<Void> open() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> close() {
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Remote replica.
   */
  private class Replica {
    private static final int BATCH_SIZE = 100;
    private final String member;
    private final DefaultCopycatStateContext context;
    private volatile Long nextIndex;
    private volatile Long matchIndex;
    private volatile Long sendIndex;
    private final TreeMap<Long, CompletableFuture<Long>> pingFutures = new TreeMap<>();
    private final Map<Long, CompletableFuture<Long>> replicateFutures = new HashMap<>(1024);

    private Replica(String member, DefaultCopycatStateContext context) {
      this.member = member;
      this.context = context;
      this.nextIndex = context.log().lastIndex() != null ? context.log().lastIndex() + 1 : null;
      this.sendIndex = nextIndex;
    }

    public CompletableFuture<Long> ping(Long index) {
      if (index != null && (matchIndex == null || index > matchIndex)) {
        return sync(index);
      }

      CompletableFuture<Long> future = new CompletableFuture<>();
      if (index != null && !pingFutures.isEmpty() && pingFutures.lastKey() >= index) {
        return pingFutures.lastEntry().getValue();
      }

      if (index != null) {
        pingFutures.put(index, future);
      }

      PingRequest request = PingRequest.builder()
        .withId(UUID.randomUUID().toString())
        .withMember(member)
        .withTerm(context.getTerm())
        .withLeader(context.getLocalMember())
        .withLogIndex(index)
        .withLogTerm(index != null && context.log().containsIndex(index) ? context.log()
          .getEntry(index)
          .getLong() : null)
        .withCommitIndex(context.getCommitIndex())
        .build();
      LOGGER.debug("{} - Sent {} to {}", context.getLocalMember(), request, member);
      pingHandler.handle(request).whenComplete((response, error) -> {
        if (error != null) {
          triggerPingFutures(index, error);
        } else {
          LOGGER.debug("{} - Received {} from {}", context.getLocalMember(), response, member);
          if (response.status().equals(Response.Status.OK)) {
            if (response.term() > context.getTerm()) {
              context.setTerm(response.term());
              transition(CopycatState.FOLLOWER);
              triggerPingFutures(index, new CopycatException("Not the leader"));
            } else if (!response.succeeded()) {
              triggerPingFutures(index, new ProtocolException("Replica not in sync"));
            } else {
              triggerPingFutures(index);
            }
          } else {
            triggerPingFutures(index, response.error());
          }
        }
      });
      return future;
    }

    /**
     * Commits the given index to the replica.
     */
    public CompletableFuture<Long> sync(long index) {
      if (matchIndex != null && index <= matchIndex) {
        return CompletableFuture.completedFuture(index);
      }

      CompletableFuture<Long> future = replicateFutures.get(index);
      if (future != null) {
        return future;
      }

      future = new CompletableFuture<>();
      replicateFutures.put(index, future);

      if (index >= sendIndex) {
        doSync();
      }
      return future;
    }

    /**
     * Performs a commit operation.
     */
    private synchronized void doSync() {
      final long prevIndex = sendIndex - 1;
      final ByteBuffer prevEntry = context.log().getEntry(prevIndex);

      // Create a list of up to ten entries to send to the follower.
      // We can only send one snapshot entry in any given request. So, if any of
      // the entries are snapshot entries, send all entries up to the snapshot and
      // then send snapshot entries individually.
      List<ByteBuffer> entries = new ArrayList<>(BATCH_SIZE);
      long lastIndex = Math.min(sendIndex + BATCH_SIZE - 1, context.log().lastIndex());
      for (long i = sendIndex; i <= lastIndex; i++) {
        entries.add(context.log().getEntry(i));
      }

      if (!entries.isEmpty()) {
        doSync(prevIndex, prevEntry, entries);
      }
    }

    /**
     * Sends a append request.
     */
    private void doSync(final long prevIndex, final ByteBuffer prevEntry, final List<ByteBuffer> entries) {
      AppendRequest request = AppendRequest.builder()
        .withId(UUID.randomUUID().toString())
        .withMember(member)
        .withTerm(context.getTerm())
        .withLeader(context.getLocalMember())
        .withLogIndex(prevIndex)
        .withLogTerm(prevEntry != null ? prevEntry.getLong() : 0)
        .withEntries(entries)
        .withCommitIndex(context.getCommitIndex())
        .build();

      sendIndex = Math.max(sendIndex + 1, prevIndex + entries.size() + 1);

      LOGGER.debug("{} - Sent {} to {}", context.getLocalMember(), request, member);
      appendHandler.handle(request).whenComplete((response, error) -> {
        if (error != null) {
          triggerReplicateFutures(prevIndex + 1, prevIndex + entries.size(), error);
        } else {
          LOGGER.debug("{} - Received {} from {}", context.getLocalMember(), response, member);
          if (response.status().equals(Response.Status.OK)) {
            if (response.succeeded()) {
              // Update the next index to send and the last index known to be replicated.
              if (!entries.isEmpty()) {
                nextIndex = nextIndex != null ? Math.max(nextIndex + 1, prevIndex + entries.size() + 1) : prevIndex + entries.size() + 1;
                matchIndex = matchIndex != null ? Math.max(matchIndex, prevIndex + entries.size()) : prevIndex + entries.size();
                triggerReplicateFutures(prevIndex + 1, prevIndex + entries.size());
                doSync();
              }
            } else {
              if (response.term() > context.getTerm()) {
                triggerReplicateFutures(prevIndex, prevIndex, new CopycatException("Not the leader"));
                transition(CopycatState.FOLLOWER);
              } else {
                // If replication failed then use the last log index indicated by
                // the replica in the response to generate a new nextIndex. This allows
                // us to skip repeatedly replicating one entry at a time if it's not
                // necessary.
                nextIndex = sendIndex = response.logIndex() != null ? Math.max(response.logIndex() + 1, context.log().firstIndex()) : prevIndex;
                doSync();
              }
            }
          } else {
            triggerReplicateFutures(prevIndex + 1, prevIndex + entries.size(), response.error());
          }
        }
      });
      doSync();
    }

    /**
     * Triggers ping futures with a completion result.
     */
    private synchronized void triggerPingFutures(Long index) {
      if (index != null) {
        NavigableMap<Long, CompletableFuture<Long>> matchFutures = pingFutures.headMap(index, true);
        for (Map.Entry<Long, CompletableFuture<Long>> entry : matchFutures.entrySet()) {
          entry.getValue().complete(index);
        }
        matchFutures.clear();
      }
    }

    /**
     * Triggers response futures with an error result.
     */
    private synchronized void triggerPingFutures(Long index, Throwable t) {
      if (index != null) {
        CompletableFuture<Long> future = pingFutures.remove(index);
        if (future != null) {
          future.completeExceptionally(t);
        }
      }
    }

    /**
     * Triggers replicate futures with an error result.
     */
    private void triggerReplicateFutures(long startIndex, long endIndex, Throwable t) {
      if (endIndex >= startIndex) {
        for (long i = startIndex; i <= endIndex; i++) {
          CompletableFuture<Long> future = replicateFutures.remove(i);
          if (future != null) {
            future.completeExceptionally(t);
          }
        }
      }
    }

    /**
     * Triggers replicate futures with a completion result
     */
    private void triggerReplicateFutures(long startIndex, long endIndex) {
      if (endIndex >= startIndex) {
        for (long i = startIndex; i <= endIndex; i++) {
          CompletableFuture<Long> future = replicateFutures.remove(i);
          if (future != null) {
            future.complete(i);
          }
        }
      }
    }
  }

}
