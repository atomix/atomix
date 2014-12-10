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
import net.kuujo.copycat.log.ActionEntry;
import net.kuujo.copycat.log.ConfigurationEntry;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Leader state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class LeaderState extends ActiveState {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderState.class);
  private ScheduledFuture<Void> currentTimer;
  private Replicator replicator;

  LeaderState(CopycatStateContext context) {
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
      .thenRunAsync(this::takeLeadership, context.executor())
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
    int count = 0;
    for (long i = context.getLastApplied() + 1; i <= context.log().lastIndex(); i++) {
      applyEntry(i);
      count++;
    }
    LOGGER.debug("{} - Applied {} entries to log", context.getLocalMember(), count);
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
  public CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
    CompletableFuture<ConfigureResponse> future = new CompletableFuture<>();

    // All cluster configuration changes must go through the leader. In order to
    // perform cluster configuration changes, the leader observes the local cluster
    // configuration if it is indeed observable. We have to be very careful about
    // the order in which cluster configuration changes occur. If two configuration
    // changes are taking place at the same time, one can overwrite the other.
    // Additionally, if a new cluster configuration immediately overwrites an old
    // configuration without first replicating a joint old/new configuration,
    // a dual-majority can result, meaning logs will ultimately become out of sync.
    // In order to avoid this, we need to perform a two-step configuration change:
    // - First log the combined current cluster configuration and new cluster
    // configuration. For instance, if a node was added to the cluster, log the
    // new configuration. If a node was removed, log the old configuration.
    // - Once the joint cluster configuration has been replicated, log and
    // sync the new configuration.
    // This two-step process ensures log consistency by ensuring that two majorities
    // cannot result from adding and removing too many nodes at once.
    LOGGER.debug("{} - Detected configuration change", context.getLocalMember());

    // First, store a copy of both the current internal cluster configuration and
    // the user defined cluster configuration. This ensures that mutable configurations
    // are not changed during the reconfiguration process which can be asynchronous.
    // Note also that we create a copy of the configuration in order to ensure that
    // polymorphic types are properly reconstructed.
    final Set<String> activeMembers = new HashSet<>(context.getMembers());

    // If another cluster configuration change is occurring right now, it's possible
    // that the two configuration changes could overlap one another. In order to
    // avoid this, we wait until all entries up to the current log index have been
    // committed before beginning the configuration change. This ensures that any
    // previous configuration changes have completed.
    LOGGER.debug("{} - Committing all entries for configuration change", context.getLocalMember());
    replicator.syncAll().whenComplete((commitIndex, commitError) -> {
      // First we need to create a joint old/new cluster configuration entry.
      // We copy the internal configuration again for safety from modifications.
      Set<String> jointMembers = new HashSet<>(activeMembers);
      jointMembers.addAll(request.members());

      // Append the joint configuration to the log. This will be replicated to
      // followers and applied to their internal cluster managers.
      ConfigurationEntry jointEntry = new ConfigurationEntry(context.getTerm(), jointMembers);
      long jointIndex = context.log().appendEntry(jointEntry);
      LOGGER.debug("{} - Appended {} to log at index {}", context.getLocalMember(), jointEntry, jointIndex);

      // Immediately after the entry is appended to the log, apply the joint
      // configuration. Cluster membership changes do not wait for commitment.
      // Since we're using a joint consensus, it's safe to work with all members
      // of both the old and new configuration without causing split elections.
      context.setMembers(jointMembers);
      LOGGER.debug("{} - Updated internal cluster configuration {}", context.getLocalMember(), jointMembers);

      // Once the cluster is updated, the replicator will be notified and update its
      // internal connections. Then we commit the joint configuration and allow
      // it to be replicated to all the nodes in the updated cluster.
      LOGGER.debug("{} - Committing all entries for configuration change", context.getLocalMember());
      replicator.sync(jointIndex).whenComplete((commitIndex2, commitError2) -> {
        // Now that we've gotten to this point, we know that the combined cluster
        // membership has been replicated to a majority of the cluster.
        // Append the new user configuration to the log and force all replicas
        // to be synchronized.
        ConfigurationEntry configEntry = new ConfigurationEntry(context.getTerm(), request.members());
        long configIndex = context.log().appendEntry(configEntry);
        LOGGER.debug("{} - Appended {} to log at index {}", context.getLocalMember(), configEntry, configIndex);

        // Again, once we've appended the new configuration to the log, update
        // the local internal configuration.
        context.setMembers(request.members());
        LOGGER.debug("{} - Updated internal cluster configuration {}", context.getLocalMember(), context.getMembers());

        // Note again that when the cluster membership changes, the replicator will
        // be notified and remove any replicas that are no longer a part of the cluster.
        // Now that the cluster and replicator have been updated, we can commit the
        // new configuration.
        LOGGER.debug("{} - Committing all entries for configuration change", context.getLocalMember());
        replicator.syncAll();
      });
    });
    return future;
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
  public CompletableFuture<SyncResponse> sync(final SyncRequest request) {
    if (request.term() > context.getTerm()) {
      return super.sync(request);
    } else if (request.term() < context.getTerm()) {
      return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
        .withId(logRequest(request).id())
        .withMember(context.getLocalMember())
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.log().lastIndex())
        .build()));
    } else {
      transition(CopycatState.FOLLOWER);
      return super.sync(request);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<CommitResponse> commit(final CommitRequest request) {
    logRequest(request);

    CompletableFuture<CommitResponse> future = new CompletableFuture<>();

    ActionInfo action = context.action(request.action());
    if (action == null) {
      future.completeExceptionally(new IllegalStateException(String.format("Invalid action %s", request.action())));
      return future;
    }


    if (action.options.isPersistent()) {
      ActionEntry entry = new ActionEntry(context.getTerm(), request.action(), request.entry());
      long index = context.log().appendEntry(entry);
      LOGGER.debug("{} - Appended {} to log at index {}", context.getLocalMember(), entry, index);
      if (action.options.isConsistent()) {
        LOGGER.debug("{} - Replicating logs up to index {} for write", context.getLocalMember(), index);
        replicator.sync(index).whenComplete((resultIndex, error) -> {
          if (error == null) {
            try {
              future.complete(logResponse(CommitResponse.builder()
                .withId(request.id())
                .withMember(context.getLocalMember())
                .withResult(action.action.execute(index, request.entry()))
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
              compactLog();
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
      } else {
        try {
          future.complete(logResponse(CommitResponse.builder()
            .withId(request.id())
            .withMember(context.getLocalMember())
            .withResult(action.action.execute(index, request.entry()))
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
          compactLog();
        }
      }
    } else if (action.options.isConsistent()) {
      long lastIndex = context.log().lastIndex();
      LOGGER.debug("{} - Synchronizing logs to index {} for read", context.getLocalMember(), lastIndex);
      replicator.ping(lastIndex).whenComplete((index, error) -> {
        if (error == null) {
          try {
            future.complete(logResponse(CommitResponse.builder()
              .withId(request.id())
              .withMember(context.getLocalMember())
              .withResult(action.action.execute(null, request.entry()))
              .build()));
          } catch (Exception e) {
            future.complete(CommitResponse.builder()
              .withId(request.id())
              .withMember(context.getLocalMember())
              .withStatus(Response.Status.ERROR)
              .withError(e)
              .build());
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
    } else {
      try {
        future.complete(CommitResponse.builder()
          .withId(request.id())
          .withMember(context.getLocalMember())
          .withResult(action.action.execute(null, request.entry()))
          .build());
      } catch (Exception e) {
        future.complete(CommitResponse.builder()
          .withId(request.id())
          .withMember(context.getLocalMember())
          .withStatus(Response.Status.ERROR)
          .withError(e)
          .build());
      }
    }
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
  private class Replicator implements Managed, Observer {
    private final CopycatStateContext context;
    private final Map<String, Replica> replicaMap;
    private final List<Replica> replicas;
    private Integer readQuorum;
    private Integer writeQuorum;
    private int quorumIndex;
    private final TreeMap<Long, CompletableFuture<Long>> commitFutures = new TreeMap<>();

    private Replicator(CopycatStateContext context) {
      this.context = context;
      this.replicaMap = new HashMap<>(context.getMembers().size());
      this.replicas = new ArrayList<>(context.getMembers().size());
    }

    /**
     * Recalculates the cluster quorum size.
     */
    private void recalculateQuorumSize() {
      int quorumSize = (int) Math.floor((replicas.size() + 1) / 2) + 1;
      quorumIndex = quorumSize > 1 ? quorumSize - 2 : 0;
    }

    @Override
    public void update(Observable o, Object arg) {
      clusterChanged((CopycatStateContext) o);
    }

    /**
     * Called when the cluster configuration changes.
     */
    private void clusterChanged(CopycatStateContext context) {
      context.getRemoteMembers().forEach(member -> {
        if (!replicaMap.containsKey(member)) {
          Replica replica = new Replica(member, context);
          replicaMap.put(member, replica);
          replicas.add(replica);
          recalculateQuorumSize();
        }
      });

      Iterator<Replica> iterator = replicas.iterator();
      while (iterator.hasNext()) {
        Replica replica = iterator.next();
        if (!context.getMembers().contains(replica.member)) {
          iterator.remove();
          replicaMap.remove(replica.member);
        }
      }
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
    public CompletableFuture<Long> ping(long index) {
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
    public CompletableFuture<Long> sync(long index) {
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
        Collections.sort(replicas, (o1, o2) -> Long.compare(o1.matchIndex, o2.matchIndex));

        // Set the current commit index as the median replicated index.
        // Since replicas is a list with zero based indexes, use the negation of
        // the required quorum size to get the index of the replica with the least
        // possible quorum replication. That replica's match index is the commit index.
        // Set the commit index. Once the commit index has been set we can run
        // all tasks up to the given commit.
        long commitIndex = replicas.get(quorumIndex).matchIndex;
        context.setCommitIndex(commitIndex);
        triggerCommitFutures(commitIndex);
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
      context.addObserver(this);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> close() {
      context.deleteObserver(this);
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Remote replica.
   */
  private class Replica {
    private static final int BATCH_SIZE = 100;
    private final String member;
    private final CopycatStateContext context;
    private volatile long nextIndex;
    private volatile long matchIndex;
    private volatile long sendIndex;
    private volatile boolean open;
    private final TreeMap<Long, CompletableFuture<Long>> pingFutures = new TreeMap<>();
    private final Map<Long, CompletableFuture<Long>> replicateFutures = new HashMap<>(1024);

    private Replica(String member, CopycatStateContext context) {
      this.member = member;
      this.context = context;
    }

    public CompletableFuture<Long> ping(long index) {
      if (index > matchIndex) {
        return sync(index);
      }

      CompletableFuture<Long> future = new CompletableFuture<>();
      if (!pingFutures.isEmpty() && pingFutures.lastKey() >= index) {
        return pingFutures.lastEntry().getValue();
      }

      pingFutures.put(index, future);

      PingRequest request = PingRequest.builder()
        .withId(UUID.randomUUID().toString())
        .withMember(member)
        .withTerm(context.getTerm())
        .withLeader(context.getLocalMember())
        .withLogIndex(index)
        .withLogTerm(context.log().getEntry(index) != null ? context.log().getEntry(index).term() : 0)
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
      if (!open) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        future.completeExceptionally(new CopycatException("Connection not open"));
        return future;
      }

      if (index <= matchIndex) {
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
      final Entry prevEntry = context.log().getEntry(prevIndex);

      // Create a list of up to ten entries to send to the follower.
      // We can only send one snapshot entry in any given request. So, if any of
      // the entries are snapshot entries, send all entries up to the snapshot and
      // then send snapshot entries individually.
      List<Entry> entries = new ArrayList<>(BATCH_SIZE);
      long lastIndex = Math.min(sendIndex + BATCH_SIZE - 1, context.log().lastIndex());
      for (long i = sendIndex; i <= lastIndex; i++) {
        entries.add(context.log().getEntry(i));
      }

      if (!entries.isEmpty()) {
        doSync(prevIndex, prevEntry, entries);
      }
    }

    /**
     * Sends a sync request.
     */
    private void doSync(final long prevIndex, final Entry prevEntry, final List<Entry> entries) {
      final long commitIndex = context.getCommitIndex();

      SyncRequest request = SyncRequest.builder()
        .withId(UUID.randomUUID().toString())
        .withMember(member)
        .withTerm(context.getTerm())
        .withLeader(context.getLocalMember())
        .withLogIndex(prevIndex)
        .withLogTerm(prevEntry != null ? prevEntry.term() : 0)
        .withEntries(entries)
        .withCommitIndex(context.getCommitIndex())
        .build();

      sendIndex = Math.max(sendIndex + 1, prevIndex + entries.size() + 1);

      LOGGER.debug("{} - Sent {} to {}", context.getLocalMember(), request, member);
      syncHandler.handle(request).whenComplete((response, error) -> {
        if (error != null) {
          triggerReplicateFutures(prevIndex + 1, prevIndex + entries.size(), error);
        } else {
          LOGGER.debug("{} - Received {} from {}", context.getLocalMember(), response, member);
          if (response.status().equals(Response.Status.OK)) {
            if (response.succeeded()) {
              // Update the next index to send and the last index known to be replicated.
              if (!entries.isEmpty()) {
                nextIndex = Math.max(nextIndex + 1, prevIndex + entries.size() + 1);
                matchIndex = Math.max(matchIndex, prevIndex + entries.size());
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
                nextIndex = sendIndex = Math.max(response.logIndex() + 1, context.log().firstIndex());
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
    private synchronized void triggerPingFutures(long index) {
      NavigableMap<Long, CompletableFuture<Long>> matchFutures = pingFutures.headMap(index, true);
      for (Map.Entry<Long, CompletableFuture<Long>> entry : matchFutures.entrySet()) {
        entry.getValue().complete(index);
      }
      matchFutures.clear();
    }

    /**
     * Triggers response futures with an error result.
     */
    private synchronized void triggerPingFutures(long index, Throwable t) {
      CompletableFuture<Long> future = pingFutures.remove(index);
      if (future != null) {
        future.completeExceptionally(t);
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
