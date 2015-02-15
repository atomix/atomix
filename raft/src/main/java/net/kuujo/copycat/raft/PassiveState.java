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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.raft.protocol.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Passive cluster status.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PassiveState extends RaftState {
  private static final int MAX_BATCH_SIZE = 1024 * 1024;
  private ScheduledFuture<?> currentTimer;

  public PassiveState(RaftContext context) {
    super(context);
  }

  @Override
  public Type type() {
    return Type.PASSIVE;
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    return super.open().thenRun(this::startSyncTimer);
  }

  /**
   * Starts the sync timer.
   */
  private void startSyncTimer() {
    LOGGER.debug("{} - Setting sync timer", context.getLocalMember().id());
    currentTimer = context.executor().scheduleAtFixedRate(this::sync, 1, context.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
  }

  /**
   * Synchronizes with random nodes via a gossip protocol.
   */
  private void sync() {
    context.checkThread();
    if (isClosed()) return;

    // Create a list of passive members. Construct a new array list since the Java 8 collectors API makes no guarantees
    // about the serializability of the returned list.
    List<RaftMember> passiveMembers = new ArrayList<>(context.getMembers().size());
    context.getMembers().stream()
      .filter(m -> m.type() == RaftMember.Type.PASSIVE && !m.id().equals(context.getLocalMember().id()))
      .forEach(passiveMembers::add);

    // Create a random list of three active members.
    Random random = new Random();
    List<RaftMember> randomMembers = new ArrayList<>(3);
    for (int i = 0; i < Math.min(passiveMembers.size(), 3); i++) {
      randomMembers.add(passiveMembers.get(random.nextInt(Math.min(passiveMembers.size(), 3))));
    }

    // Increment the local member version in the vector clock.
    context.setVersion(context.getVersion() + 1);

    Set<String> synchronizing = new HashSet<>();
    // For each active member, send membership info to the member.
    for (RaftMember member : randomMembers) {
      // If we're already synchronizing with the given node then skip the synchronization. This is possible in the event
      // that we began sending sync requests during another gossip pass and continue to send entries recursively.
      if (synchronizing.add(member.id())) {
        recursiveSync(member).whenComplete((result, error) -> synchronizing.remove(member.id()));
      }
    }
  }

  /**
   * Recursively sends sync request to the given member.
   */
  private CompletableFuture<Void> recursiveSync(RaftMember member) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    recursiveSync(member, false, future);
    return future;
  }

  /**
   * Recursively sends sync requests to the given member.
   */
  private void recursiveSync(RaftMember member, boolean requireEntries, CompletableFuture<Void> future) {
    // Get a list of entries up to 1MB in size.
    List<ByteBuffer> entries = new ArrayList<>(1024);
    Long firstIndex = null;
    if (!context.log().isEmpty() && context.getCommitIndex() != null) {
      firstIndex = Math.max(member.index() != null ? member.index() + 1 : context.log().firstIndex(), context.log().lastIndex());
      long index = firstIndex;
      int size = 0;
      while (size < MAX_BATCH_SIZE && index <= context.getCommitIndex()) {
        ByteBuffer entry = context.log().getEntry(index);
        size += entry.limit();
        entries.add(entry);
        index++;
      }
    }

    // If we have entries to send or if entries are not required for this request then send the sync request.
    if (!requireEntries || !entries.isEmpty()) {
      SyncRequest request = SyncRequest.builder()
        .withId(member.id())
        .withLeader(context.getLeader())
        .withTerm(context.getTerm())
        .withLogIndex(member.index())
        .withFirstIndex(firstIndex != null && firstIndex.equals(context.log().firstIndex()))
        .withMembers(context.getMembers())
        .withEntries(entries)
        .build();

      LOGGER.debug("{} - Sending sync request to {}", context.getLocalMember().id(), member.id());
      syncHandler.apply(request).whenCompleteAsync((response, error) -> {
        context.checkThread();
        // Always check if the context is still open in order to prevent race conditions in asynchronous callbacks.
        if (isOpen()) {
          if (error == null) {
            // If the response succeeded, update membership info with the target node's membership.
            if (response.status() == Response.Status.OK) {
              context.updateMembers(response.members());
              recursiveSync(member, true, future);
            } else {
              LOGGER.warn("{} - Received error response from {}", context.getLocalMember().id(), member.id());
              future.completeExceptionally(response.error());
            }
          } else {
            // If the request failed then record the member as INACTIVE.
            LOGGER.warn("{} - Sync to {} failed: {}", context.getLocalMember().id(), member, error.getMessage());
            future.completeExceptionally(error);
          }
        }
      }, context.executor());
    } else {
      future.complete(null);
    }
  }

  @Override
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    context.checkThread();

    if (request.term() > context.getTerm()) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
    } else if (request.term() == context.getTerm() && context.getLeader() == null && request.leader() != null) {
      context.setLeader(request.leader());
    }

    // Increment the local vector clock version and update cluster members.
    context.setVersion(context.getVersion() + 1);
    context.updateMembers(request.members());

    // If the local log doesn't contain the previous index and the given index is not the first index in the
    // requestor's log then reply immediately.
    if (!request.firstIndex() && request.logIndex() != null && !context.log().containsIndex(request.logIndex())) {
      return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
        .withId(context.getLocalMember().id())
        .withMembers(context.getMembers())
        .build()));
    }

    // If the given previous log index is not null and the requestor indicated that the first entry in the entry set
    // is the first entry in the log then roll over the local log to a new segment with the given starting index.
    Long rollOverIndex = null;
    if (!request.entries().isEmpty() && request.logIndex() != null && request.firstIndex()) {
      rollOverIndex = request.logIndex() + 1;
      try {
        context.log().rollOver(rollOverIndex);
      } catch (IOException e) {
        LOGGER.error("{} - Failed to roll over log", context.getLocalMember().id());
        return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
          .withId(context.getLocalMember().id())
          .withMembers(context.getMembers())
          .build()));
      }
    }

    // Iterate through provided entries and append any that are missing from the log. Only committed entries are
    // replicated via gossip, so we don't have to worry about consistency checks here.
    for (int i = 0; i < request.entries().size(); i++) {
      long index = request.logIndex() != null ? request.logIndex() + i + 1 : i + 1;
      if (!context.log().containsIndex(index)) {
        ByteBuffer entry = request.entries().get(i);
        try {
          context.log().appendEntry(entry);
          context.setCommitIndex(index);

          // Extract a view of the entry after the entry term.
          long term = entry.getLong();
          ByteBuffer userEntry = entry.slice();

          try {
            context.commitHandler().commit(term, index, userEntry);
          } catch (Exception e) {
          }

          context.setLastApplied(index);
          LOGGER.debug("{} - Appended {} to log at index {}", context.getLocalMember().id(), entry, index);
        } catch (IOException e) {
          break;
        }
      }
    }

    // If the given previous log index is not null and the requestor indicates that the first entry in the entry set
    // is the first entry in the log then compact the log up to the given first index.
    try {
      if (rollOverIndex != null) {
        context.log().compact(rollOverIndex);
      }
    } catch (IOException e) {
      LOGGER.error("{} - Failed to compact log", context.getLocalMember().id());
    } finally {
      // Flush the log to disk.
      context.log().flush();
    }

    // Reply with the updated vector clock.
    return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
      .withId(context.getLocalMember().id())
      .withMembers(context.getMembers())
      .build()));
  }

  @Override
  public CompletableFuture<JoinResponse> join(JoinRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
        .withId(context.getLocalMember().id())
        .withStatus(Response.Status.ERROR)
        .withError(new IllegalStateException("Not the leader"))
        .build()));
    } else {
      return joinHandler.apply(JoinRequest.builder(request).withId(context.getLeader()).build());
    }
  }

  @Override
  public CompletableFuture<PromoteResponse> promote(PromoteRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(PromoteResponse.builder()
        .withId(context.getLocalMember().id())
        .withStatus(Response.Status.ERROR)
        .withError(new IllegalStateException("Not the leader"))
        .build()));
    } else {
      return promoteHandler.apply(PromoteRequest.builder(request).withId(context.getLeader()).build());
    }
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
        .withId(context.getLocalMember().id())
        .withStatus(Response.Status.ERROR)
        .withError(new IllegalStateException("Not the leader"))
        .build()));
    } else {
      return leaveHandler.apply(LeaveRequest.builder(request).withId(context.getLeader()).build());
    }
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    context.checkThread();
    logRequest(request);
    // If the request allows inconsistency, immediately execute the query and return the result.
    if (request.consistency() == Consistency.WEAK) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
        .withId(context.getLocalMember().id())
        .withResult(context.commitHandler().commit(context.getTerm(), null, request.entry()))
        .build()));
    } else if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
        .withId(context.getLocalMember().id())
        .withStatus(Response.Status.ERROR)
        .withError(new IllegalStateException("Not the leader"))
        .build()));
    } else {
      return queryHandler.apply(QueryRequest.builder(request).withId(context.getLeader()).build());
    }
  }

  @Override
  public CompletableFuture<CommandResponse> command(CommandRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
        .withId(context.getLocalMember().id())
        .withStatus(Response.Status.ERROR)
        .withError(new IllegalStateException("Not the leader"))
        .build()));
    } else {
      return commitHandler.apply(CommandRequest.builder(request).withId(context.getLeader()).build());
    }
  }

  /**
   * Cancels the sync timer.
   */
  private void cancelSyncTimer() {
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling sync timer", context.getLocalMember().id());
      currentTimer.cancel(false);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelSyncTimer);
  }

  @Override
  public String toString() {
    return String.format("%s[context=%s]", getClass().getSimpleName(), context);
  }

}
