/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.raft.Member;
import net.kuujo.copycat.raft.RaftServer;
import net.kuujo.copycat.raft.protocol.request.LeaveRequest;
import net.kuujo.copycat.raft.protocol.response.LeaveResponse;
import net.kuujo.copycat.raft.protocol.response.Response;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Leave state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaveState extends InactiveState {
  private ScheduledFuture<?> leaveFuture;

  public LeaveState(ServerContext context) {
    super(context);
  }

  @Override
  public CompletableFuture<AbstractState> open() {
    return super.open().thenRun(this::startLeaveTimeout).thenRun(this::leave).thenApply(v -> this);
  }

  @Override
  public RaftServer.State type() {
    return RaftServer.State.LEAVE;
  }

  /**
   * Sets a leave timeout.
   */
  private void startLeaveTimeout() {
    leaveFuture = context.getContext().schedule(() -> {
      if (isOpen()) {
        LOGGER.warn("{} - Failed to leave the cluster in {} milliseconds", context.getMember().id(), context.getElectionTimeout());
        transition(RaftServer.State.INACTIVE);
      }
    }, context.getElectionTimeout().toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Starts leaving the cluster.
   */
  private void leave() {
    if (context.getLeader() == null) {
      leave(context.getLeader(), context.getCluster().getActiveMembers().iterator());
    } else {
      Iterator<MemberState> iterator = context.getCluster().getActiveMembers().iterator();
      if (iterator.hasNext()) {
        leave(iterator.next().getMember(), iterator);
      } else {
        LOGGER.debug("{} - Failed to leave the cluster", context.getMember().id());
        transition(RaftServer.State.INACTIVE);
      }
    }
  }

  /**
   * Recursively attempts to leave the cluster.
   */
  private void leave(Member member, Iterator<MemberState> iterator) {
    LOGGER.debug("{} - Attempting to leave via {}", context.getMember().id(), member);

    context.getConnections().getConnection(member).thenAccept(connection -> {
      if (isOpen()) {
        LeaveRequest request = LeaveRequest.builder()
          .withMember(context.getMember())
          .build();
        connection.<LeaveRequest, LeaveResponse>send(request).whenComplete((response, error) -> {
          if (isOpen()) {
            if (error == null) {
              if (response.status() == Response.Status.OK) {
                LOGGER.info("{} - Successfully left via {}", context.getMember().id(), member);
                transition(RaftServer.State.INACTIVE);
              } else {
                LOGGER.debug("{} - Failed to leave {}", context.getMember().id(), member);
                if (iterator.hasNext()) {
                  leave(iterator.next().getMember(), iterator);
                } else {
                  transition(RaftServer.State.INACTIVE);
                }
              }
            } else {
              LOGGER.debug("{} - Failed to leave {}", context.getMember().id(), member);
              if (iterator.hasNext()) {
                leave(iterator.next().getMember(), iterator);
              } else {
                transition(RaftServer.State.INACTIVE);
              }
            }
          }
        });
      }
    });
  }

  /**
   * Cancels the leave timeout.
   */
  private void cancelLeaveTimer() {
    if (leaveFuture != null) {
      LOGGER.info("{} - Cancelling leave timeout", context.getMember().id());
      leaveFuture.cancel(false);
      leaveFuture = null;
    }
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelLeaveTimer);
  }

}
