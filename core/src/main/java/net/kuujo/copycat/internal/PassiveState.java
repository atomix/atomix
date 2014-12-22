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
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.protocol.MemberInfo;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(PassiveState.class);
  private ScheduledFuture<?> currentTimer;

  public PassiveState(CopycatStateContext context) {
    super(context);
  }

  @Override
  public CopycatState state() {
    return CopycatState.PASSIVE;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
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
    setSyncTimer();
  }

  /**
   * Sets the sync timer.
   */
  private void setSyncTimer() {
    currentTimer = context.executor().schedule(() -> {
      sync();
      setSyncTimer();
    }, context.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
  }

  /**
   * Synchronizes with random nodes via a gossip protocol.
   */
  private void sync() {
    // Create a list of currently active members.
    List<MemberInfo> activeMembers = new ArrayList<>(context.getMembers().size());
    for (MemberInfo member : context.getMembers()) {
      if (!member.equals(context.getLocalMember())
        && (context.getLocalMember().type() == Member.Type.MEMBER && member.type() == Member.Type.LISTENER)
        || (context.getLocalMember().type() == Member.Type.LISTENER && member.type() == Member.Type.MEMBER)
        && (member.state() == Member.State.SUSPICIOUS || member.state() == Member.State.ALIVE)) {
        activeMembers.add(member);
      }
    }

    // Create a random list of three active members.
    Random random = new Random();
    List<MemberInfo> randomMembers = new ArrayList<>(3);
    for (int i = 0; i < Math.min(activeMembers.size(), 3); i++) {
      randomMembers.add(activeMembers.get(random.nextInt(Math.min(activeMembers.size() - 1, 2))));
    }

    // Increment the local member version in the vector clock.
    context.setVersion(context.getVersion() + 1);

    // For each active member, send membership info to the member.
    for (MemberInfo member : randomMembers) {
      LOGGER.debug("{} - sending sync request to {}", context.getLocalMember(), member.uri());
      List<ByteBuffer> entries = context.log().getEntries(member.index() != null ? member.index() : 1, Math.min(member.index() + 100, context.getCommitIndex()));
      syncHandler.handle(SyncRequest.builder()
        .withId(UUID.randomUUID().toString())
        .withLeader(context.getLeader())
        .withTerm(context.getTerm())
        .withLogIndex(member.index())
        .withCommitIndex(context.getCommitIndex())
        .withMembers(context.getMembers())
        .withEntries(entries)
        .build()).whenComplete((response, error) -> {
        if (error == null) {
          // If the response succeeded, update membership info with the target node's membership.
          if (response.status() == Response.Status.OK) {
            context.setMembers(response.members());
            member.succeed();
          } else {
            LOGGER.warn("{} - received error response from {}", context.getLocalMember(), member.uri());
          }
        } else {
          // If the request failed then record the member as INACTIVE.
          LOGGER.warn("{} - sync to {} failed", context.getLocalMember(), member.uri());
          member.fail(context.getLocalMember().uri());
        }
      });
    }
  }

  @Override
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    if (request.term() > context.getTerm()) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
    }

    // Increment the local vector clock version and update cluster members.
    context.setVersion(context.getVersion() + 1);
    context.setMembers(request.members());

    for (int i = 0; i < request.entries().size(); i++) {
      long index = request.logIndex() != null ? request.logIndex() + i + 1 : i + 1;
      if (!context.log().containsIndex(index)) {
        if ((index == 1 && context.log().lastIndex() != null) || (index > 1 && context.log().lastIndex() != index - 1)) {
          return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
            .withId(logRequest(request).id())
            .withUri(context.getLocalMember().uri())
            .withMembers(context.getMembers())
            .build()));
        }
        ByteBuffer entry = request.entries().get(i);
        context.log().appendEntry(entry);
        context.setCommitIndex(index);
        context.consumer().apply(index, entry);
        context.setLastApplied(index);
      }
    }

    return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
      .withId(logRequest(request).id())
      .withUri(context.getLocalMember().uri())
      .withMembers(context.getMembers())
      .build()));
  }

  /**
   * Cancels the sync timer.
   */
  private void cancelSyncTimer() {
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling sync timer", context.getLocalMember());
      currentTimer.cancel(true);
    }
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelSyncTimer);
  }

}
