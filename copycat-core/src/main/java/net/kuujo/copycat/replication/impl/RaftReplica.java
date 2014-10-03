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
package net.kuujo.copycat.replication.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import net.kuujo.copycat.CopyCatException;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.impl.RaftEntry;
import net.kuujo.copycat.log.impl.SnapshotEntry;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.state.impl.Follower;
import net.kuujo.copycat.state.impl.RaftStateContext;

/**
 * Raft-based replica implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class RaftReplica {
  private static final int BATCH_SIZE = 100;
  private final Member member;
  private final RaftStateContext state;
  private final Log log;
  private volatile long nextIndex;
  private volatile long matchIndex;
  private volatile long sendIndex;
  private volatile boolean open;
  private final TreeMap<Long, CompletableFuture<Long>> pingFutures = new TreeMap<>();
  private final Map<Long, CompletableFuture<Long>> commitFutures = new ConcurrentHashMap<>();

  public RaftReplica(Member member, RaftStateContext state) {
    this.member = member;
    this.state = state;
    this.log = state.log();
    this.nextIndex = log.lastIndex();
    this.sendIndex = nextIndex;
  }

  /**
   * Returns the replica member.
   *
   * @return The replica member.
   */
  Member member() {
    return member;
  }

  /**
   * Returns the next index to be sent to the replica.
   */
  long nextIndex() {
    return nextIndex;
  }

  /**
   * Returns the index of the last entry known to be replicated to the replica.
   */
  long matchIndex() {
    return matchIndex;
  }

  /**
   * Opens the connection to the replica.
   */
  CompletableFuture<Void> open() {
    if (open == false) {
      return member.protocol().client().connect().whenComplete((result, error) -> {
        if (error == null) {
          open = true;
        }
      });
    }
    return CompletableFuture.completedFuture(null);
  }


  /**
   * Pings the replica.
   */
  synchronized CompletableFuture<Long> ping(long index) {
    if (!open) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      future.completeExceptionally(new CopyCatException("Connection not open"));
      return future;
    }

    CompletableFuture<Long> future = new CompletableFuture<>();
    if (!pingFutures.isEmpty() && pingFutures.lastKey() >= index) {
      return pingFutures.lastEntry().getValue();
    }

    pingFutures.put(index, future);

    PingRequest request = new PingRequest(state.nextCorrelationId(), state.getCurrentTerm(), state.clusterConfig().getLocalMember(), index, log.containsEntry(index) ? log.<RaftEntry>getEntry(index).term() : 0, state.getCommitIndex());
    member.protocol().client().ping(request).whenCompleteAsync((response, error) -> {
      if (error != null) {
        triggerPingFutures(index, error);
      } else {
        if (response.status().equals(Response.Status.OK)) {
          if (response.term() > state.getCurrentTerm()) {
            state.setCurrentTerm(response.term());
            state.setCurrentLeader(null);
            state.transition(Follower.class);
            triggerPingFutures(index, new CopyCatException("Not the leader"));
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
  CompletableFuture<Long> commit(long index) {
    if (!open) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      future.completeExceptionally(new CopyCatException("Connection not open"));
      return future;
    }

    if (index <= matchIndex) {
      return CompletableFuture.completedFuture(index);
    }

    CompletableFuture<Long> future = new CompletableFuture<>();
    commitFutures.put(index, future);

    if (index >= sendIndex) {
      doCommit();
    }
    return future;
  }

  /**
   * Performs a commit operation.
   */
  private synchronized void doCommit() {
    final long prevIndex = sendIndex - 1;
    final RaftEntry prevEntry = log.getEntry(prevIndex);

    // Create a list of up to ten entries to send to the follower.
    // We can only send one snapshot entry in any given request. So, if any of
    // the entries are snapshot entries, send all entries up to the snapshot and
    // then send snapshot entries individually.
    List<RaftEntry> entries = new ArrayList<>();
    long lastIndex = Math.min(sendIndex + BATCH_SIZE, log.lastIndex());
    for (long i = sendIndex; i <= lastIndex; i++) {
      RaftEntry entry = log.getEntry(i);
      if (entry instanceof SnapshotEntry) {
        if (entries.isEmpty()) {
          doSync(prevIndex, prevEntry, Arrays.asList(entry));
        } else {
          doSync(prevIndex, prevEntry, entries);
        }
        return;
      } else {
        entries.add(entry);
      }
    }

    if (!entries.isEmpty()) {
      doSync(prevIndex, prevEntry, entries);
    }
  }

  /**
   * Sends a sync request.
   */
  private void doSync(final long prevIndex, final RaftEntry prevEntry, final List<RaftEntry> entries) {
    final long commitIndex = state.getCommitIndex();

    SyncRequest request = new SyncRequest(state.nextCorrelationId(), state.getCurrentTerm(), state.clusterConfig().getLocalMember(), prevIndex, prevEntry != null ? prevEntry.term() : 0, entries, commitIndex);

    sendIndex = Math.max(sendIndex + 1, prevIndex + entries.size() + 1);

    member.protocol().client().sync(request).whenComplete((response, error) -> {
      if (error != null) {
        triggerCommitFutures(prevIndex+1, prevIndex+entries.size(), error);
      } else {
        if (response.status().equals(Response.Status.OK)) {
          if (response.succeeded()) {
            // Update the next index to send and the last index known to be replicated.
            if (!entries.isEmpty()) {
              nextIndex = Math.max(nextIndex + 1, prevIndex + entries.size() + 1);
              matchIndex = Math.max(matchIndex, prevIndex + entries.size());
              triggerCommitFutures(prevIndex+1, prevIndex+entries.size());
              doCommit();
            }
          } else {
            if (response.term() > state.getCurrentTerm()) {
              triggerCommitFutures(prevIndex, prevIndex, new CopyCatException("Not the leader"));
              state.transition(Follower.class);
            } else {
              // If replication failed then use the last log index indicated by
              // the replica in the response to generate a new nextIndex. This allows
              // us to skip repeatedly replicating one entry at a time if it's not
              // necessary.
              nextIndex = sendIndex = response.lastLogIndex() + 1;
              doCommit();
            }
          }
        } else {
          triggerCommitFutures(prevIndex+1, prevIndex+entries.size(), response.error());
        }
      }
    });
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
   * Triggers commit futures with an error result.
   */
  private void triggerCommitFutures(long startIndex, long endIndex, Throwable t) {
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
   * Triggers commit futures with a completion result
   */
  private void triggerCommitFutures(long startIndex, long endIndex) {
    if (endIndex >= startIndex) {
      for (long i = startIndex; i <= endIndex; i++) {
        CompletableFuture<Long> future = commitFutures.remove(i);
        if (future != null) {
          future.complete(i);
        }
      }
    }
  }

  /**
   * Closes the replica.
   */
  CompletableFuture<Void> close() {
    return member.protocol().client().close().whenComplete((result, error) -> open = false);
  }

  @Override
  public int hashCode() {
    return 37 + member.uri().hashCode() * 7;
  }

  @Override
  public String toString() {
    return member.toString();
  }

}
