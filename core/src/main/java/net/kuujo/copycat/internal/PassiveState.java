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
import net.kuujo.copycat.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
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
  private final Membership membership;

  public PassiveState(CopycatStateContext context) {
    super(context);
    this.membership = new Membership(context.getLocalMember(), context.getMembers());
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
    Collection<MemberInfo> members = membership.members();
    List<MemberInfo> activeMembers = new ArrayList<>(members.size());
    for (MemberInfo member : members) {
      if (member.status() == MemberInfo.STATUS_ACTIVE) {
        activeMembers.add(member);
      }
    }

    // Create a random list of three active members.
    Random random = new Random();
    List<MemberInfo> randomMembers = new ArrayList<>();
    for (int i = 0; i < Math.min(activeMembers.size(), 3); i++) {
      randomMembers.add(activeMembers.get(random.nextInt(Math.min(activeMembers.size() - 1, 2))));
    }

    // For each active member, send membership info to the member.
    for (MemberInfo member : randomMembers) {
      LOGGER.debug("{} - sending sync request to {}", context.getLocalMember(), member.uri());
      List<ByteBuffer> entries = context.log().getEntries(member.index(), Math.min(member.index() + 100, context.log().lastIndex()));
      syncHandler.handle(SyncRequest.builder()
        .withId(UUID.randomUUID().toString())
        .withMember(member.uri())
        .withMembership(membership.increment())
        .withEntries(entries)
        .build()).whenComplete((response, error) -> {
        if (error == null) {
          // If the response succeeded, update membership info with the target node's membership.
          if (response.status() == Response.Status.OK) {
            membership.update(response.membership());
          } else {
            LOGGER.warn("{} - received error response from {}", context.getLocalMember(), member.uri());
          }
        } else {
          // If the request failed then record the member as INACTIVE.
          LOGGER.warn("{} - sync to {} failed", context.getLocalMember(), member.uri());
          member.update(new MemberInfo(member.uri(), member.version(), member.index(), member.leader(), member.term(), MemberInfo.STATUS_INACTIVE));
        }
      });
    }
  }

  @Override
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    membership.update(request.membership());
    return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
      .withId(logRequest(request).id())
      .withMember(context.getLocalMember())
      .withMembership(membership)
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
