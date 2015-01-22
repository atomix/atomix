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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.protocol.ReplicaInfo;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Passive cluster state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PassiveState extends AbstractState {
  private ScheduledFuture<?> currentTimer;

  public PassiveState(CopycatStateContext context) {
    super(context);
  }

  @Override
  public CopycatState state() {
    return CopycatState.PASSIVE;
  }

  @Override
  public CompletableFuture<Void> open() {
    return super.open().thenRun(this::startSyncTimer);
  }

  /**
   * Starts the sync timer.
   */
  private void startSyncTimer() {
    LOGGER.debug("{} - Setting sync timer", context.getLocalMember());
    currentTimer = context.executor().scheduleAtFixedRate(this::sync, 1, context.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
  }

  /**
   * Synchronizes with random nodes via a gossip protocol.
   */
  private void sync() {
    context.checkThread();
    if (isClosed()) return;

    // Create a list of currently active members.
    List<ReplicaInfo> activeMembers = new ArrayList<>(context.getMembers().size());
    for (String uri : context.getMembers()) {
      if (!uri.equals(context.getLocalMember())) {
        ReplicaInfo member = context.getMemberInfo(uri);
        if (member == null) {
          member = new ReplicaInfo(uri);
          context.addMemberInfo(member);
        }
        // If the local node is an active member of the cluster, only gossip with passive members. If the local node
        // is a passive member of the cluster, gossip with both active and passive members.
        if ((context.getActiveMembers().contains(context.getLocalMember()) && !context.getActiveMembers().contains(member.getUri()))
          || !context.getActiveMembers().contains(context.getLocalMember())) {
          activeMembers.add(member);
        }
      }
    }

    // Create a random list of three active members.
    Random random = new Random();
    List<ReplicaInfo> randomMembers = new ArrayList<>(3);
    for (int i = 0; i < Math.min(activeMembers.size(), 3); i++) {
      randomMembers.add(activeMembers.get(random.nextInt(Math.min(activeMembers.size(), 3))));
    }

    // Increment the local member version in the vector clock.
    context.setVersion(context.getVersion() + 1);

    // For each active member, send membership info to the member.
    for (ReplicaInfo member : randomMembers) {
      LOGGER.debug("{} - sending sync request to {}", context.getLocalMember(), member.getUri());

      // Get a list of entries up to 1MB in size.
      List<ByteBuffer> entries = new ArrayList<>(1024);
      Long firstIndex = null;
      if (!context.log().isEmpty()) {
        firstIndex = Math.max(member.getIndex() != null ? member.getIndex() + 1 : context.log().firstIndex(), context.log().lastIndex());
        long index = firstIndex;
        int size = 0;
        while (size < 1024 * 1024 && index <= context.getCommitIndex()) {
          ByteBuffer entry = context.log().getEntry(index);
          size += entry.limit();
          entries.add(entry);
          index++;
        }
      }

      syncHandler.handle(SyncRequest.builder()
        .withId(UUID.randomUUID().toString())
        .withUri(member.getUri())
        .withLeader(context.getLeader())
        .withTerm(context.getTerm())
        .withLogIndex(member.getIndex())
        .withFirstIndex(firstIndex != null && firstIndex.equals(context.log().firstIndex()))
        .withMembers(context.getMemberInfo())
        .withEntries(entries)
        .build()).whenComplete((response, error) -> {
        context.checkThread();
        // Always check if the context is still open in order to prevent race conditions in asynchronous callbacks.
        if (isOpen()) {
          if (error == null) {
            // If the response succeeded, update membership info with the target node's membership.
            if (response.status() == Response.Status.OK) {
              context.setMemberInfo(response.members());
            } else {
              LOGGER.warn("{} - received error response from {}", context.getLocalMember(), member.getUri());
            }
          } else {
            // If the request failed then record the member as INACTIVE.
            LOGGER.warn("{} - sync to {} failed", context.getLocalMember(), member);
          }
        }
      });
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
    context.setMemberInfo(request.members());

    // If the local log doesn't contain the previous index and the given index is not the first index in the
    // requestor's log then reply immediately.
    if (!request.firstIndex() && request.logIndex() != null && !context.log().containsIndex(request.logIndex())) {
      return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
        .withId(logRequest(request).id())
        .withUri(context.getLocalMember())
        .withMembers(context.getMemberInfo())
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
        LOGGER.error("{} - Failed to roll over log", context.getLocalMember());
        return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
          .withId(logRequest(request).id())
          .withUri(context.getLocalMember())
          .withMembers(context.getMemberInfo())
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
          entry.position(8);
          ByteBuffer userEntry = entry.slice();

          try {
            context.consumer().apply(index, userEntry);
          } catch (Exception e) {
          }

          context.setLastApplied(index);
          LOGGER.debug("{} - Appended {} to log at index {}", context.getLocalMember(), entry, index);
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
      LOGGER.error("{} - Failed to compact log", context.getLocalMember());
    } finally {
      // Flush the log to disk.
      context.log().flush();
    }

    // Reply with the updated vector clock.
    return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
      .withId(logRequest(request).id())
      .withUri(context.getLocalMember())
      .withMembers(context.getMemberInfo())
      .build()));
  }

  /**
   * Cancels the sync timer.
   */
  private void cancelSyncTimer() {
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling sync timer", context.getLocalMember());
      currentTimer.cancel(false);
    }
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelSyncTimer);
  }

}
