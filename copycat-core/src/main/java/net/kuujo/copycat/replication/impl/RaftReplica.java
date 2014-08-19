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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import net.kuujo.copycat.CopyCatException;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.impl.SnapshotEntry;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.replication.Replica;
import net.kuujo.copycat.state.impl.Follower;
import net.kuujo.copycat.state.impl.RaftStateContext;

/**
 * Raft-based replica implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftReplica implements Replica {
  private final Member member;
  private final RaftStateContext state;
  private final Log log;
  private volatile long nextIndex;
  private volatile long matchIndex = 1;
  private volatile long nextSendIndex;
  private volatile boolean open;
  private final AtomicBoolean pinging = new AtomicBoolean();
  private final List<CompletableFuture<Long>> pingFutures = new CopyOnWriteArrayList<>();
  private final Map<Long, CompletableFuture<Long>> commitFutures = new ConcurrentHashMap<>();

  public RaftReplica(Member member, RaftStateContext state) {
    this.member = member;
    this.state = state;
    this.log = state.context().log();
    this.nextIndex = log.lastIndex();
    this.nextSendIndex = nextIndex;
  }

  @Override
  public Member member() {
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

  @Override
  public CompletableFuture<Long> ping(long index) {
    if (!open) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      future.completeExceptionally(new CopyCatException("Connection not open"));
      return future;
    }

    CompletableFuture<Long> future = new CompletableFuture<>();
    if (pinging.compareAndSet(false, true)) {
      AppendEntriesRequest request = new AppendEntriesRequest(state.nextCorrelationId(), state.getCurrentTerm(), state.cluster().getLocalMember(), nextIndex-1, state.context().log().containsEntry(nextIndex-1) ? state.context().log().getEntry(nextIndex-1).term() : 0, new ArrayList<Entry>(), state.getCommitIndex());
      member.protocol().client().appendEntries(request).whenCompleteAsync((response, error) -> {
        pinging.set(false);
        if (error != null) {
          triggerPingFutures(error);
        } else {
          if (response.status().equals(Response.Status.OK)) {
            if (response.term() > state.getCurrentTerm()) {
              state.setCurrentTerm(response.term());
              state.setCurrentLeader(null);
              state.transition(Follower.class);
              triggerPingFutures(new CopyCatException("Not the leader"));
            } else {
              triggerPingFutures();
            }
          } else {
            triggerPingFutures(response.error());
          }
        }
      });
    } else {
      pingFutures.add(future);
    }
    return future;
  }

  @Override
  public CompletableFuture<Long> commit(long index) {
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

    if (index >= nextSendIndex) {
      doCommit();
    }
    return future;
  }

  /**
   * Performs a commit operation.
   */
  private synchronized void doCommit() {
    final long prevIndex = nextSendIndex - 1;
    final Entry prevEntry = log.getEntry(prevIndex);

    // Create a list of up to ten entries to send to the follower.
    // We can only send one snapshot entry in any given request. So, if any of
    // the entries are snapshot entries, send all entries up to the snapshot and
    // then send snapshot entries individually.
    List<Entry> entries = new ArrayList<>();
    long lastIndex = nextSendIndex + 10 > log.lastIndex() ? log.lastIndex() : nextSendIndex + 10;
    for (long i = nextSendIndex; i <= lastIndex; i++) {
      Entry entry = log.getEntry(i);
      if (entry instanceof SnapshotEntry) {
        if (entries.isEmpty()) {
          doAppendEntries(prevIndex, prevEntry, Arrays.asList(entry));
        } else {
          doAppendEntries(prevIndex, prevEntry, entries);
        }
        return;
      } else {
        entries.add(entry);
      }
    }
    doAppendEntries(prevIndex, prevEntry, entries);
  }

  /**
   * Sends an append entries request.
   */
  private synchronized void doAppendEntries(final long prevIndex, final Entry prevEntry, final List<Entry> entries) {
    final long commitIndex = state.getCommitIndex();

    AppendEntriesRequest request = new AppendEntriesRequest(state.nextCorrelationId(), state.getCurrentTerm(), state.cluster().getLocalMember(), prevIndex, prevEntry != null ? prevEntry.term() : 0, entries, commitIndex);

    nextSendIndex = Math.max(nextSendIndex + 1, prevIndex + entries.size() + 1);

    member.protocol().client().appendEntries(request).whenComplete((response, error) -> {
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
            // If replication failed then decrement the next index and attempt to
            // retry replication. If decrementing the next index would result in
            // a next index of 0 then something must have gone wrong. Revert to
            // a follower.
            if (nextIndex-1 == 0) {
              state.transition(Follower.class);
            } else {
              // If we were attempting to replicate log entries and not just
              // sending a commit index or if we didn't have any log entries
              // to replicate then decrement the next index. The node we were
              // attempting to sync is not up to date.
              nextIndex--;
              nextSendIndex = nextIndex;
              doCommit();
            }
          }
        } else {
          triggerCommitFutures(prevIndex+1, prevIndex+entries.size(), response.error());
        }
      }
    });

    if (nextSendIndex < log.lastIndex()) {
      doCommit();
    }
  }

  /**
   * Triggers ping futures with a completion result.
   */
  private void triggerPingFutures() {
    for (CompletableFuture<Long> future : pingFutures) {
      future.complete(matchIndex);
    }
    pingFutures.clear();
  }

  /**
   * Triggers response futures with an error result.
   */
  private void triggerPingFutures(Throwable t) {
    for (CompletableFuture<Long> future : pingFutures) {
      future.completeExceptionally(t);
    }
    pingFutures.clear();
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
