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
package net.kuujo.copycat.protocol.raft;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.protocol.raft.rpc.*;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Remote state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RemoteState extends RaftState {
  private final AtomicBoolean statusCheck = new AtomicBoolean();
  private final Random random = new Random();
  private ScheduledFuture<?> currentTimer;

  public RemoteState(Raft context) {
    super(context);
  }

  @Override
  public Type type() {
    return Type.REMOTE;
  }

  @Override
  public synchronized CompletableFuture<RaftState> open() {
    return super.open().thenRun(this::startStatusTimer).thenApply(v -> this);
  }

  /**
   * Starts the status timer.
   */
  private void startStatusTimer() {
    LOGGER.debug("{} - Setting status timer", context.cluster().member().id());
    currentTimer = context.context().scheduleAtFixedRate(this::status, 1, context.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
  }

  /**
   * Sends a status request to a random member.
   */
  private void status() {
    if (statusCheck.compareAndSet(false, true)) {
      Member member;
      if (context.getLeader() != 0) {
        member = context.cluster().member(context.getLeader());
      } else {
        List<Member> members = context.cluster().members().stream().filter(m -> m.type() == Member.Type.ACTIVE).collect(Collectors.toList());
        member = members.get(random.nextInt(members.size()));
      }

      StatusRequest request = StatusRequest.builder()
        .withId(context.cluster().member().id())
        .build();
      member.<StatusRequest, StatusResponse>send(context.topic(), request).whenComplete((response, error) -> {
        context.checkThread();

        if (isOpen()) {
          if (error == null) {
            if (response.term() > context.getTerm()) {
              context.setTerm(response.term());
              context.setLeader(response.leader());
            } else if (context.getLeader() == 0) {
              context.setLeader(response.leader());
            }
          }
        }
        statusCheck.set(false);
      });
    }
  }

  @Override
  protected CompletableFuture<AppendResponse> append(AppendRequest request) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(AppendResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<SyncResponse> sync(SyncRequest request) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(SyncResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<PollResponse> poll(PollRequest request) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<SubmitResponse> submit(SubmitRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == 0) {
      return CompletableFuture.completedFuture(logResponse(SubmitResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return context.cluster().member(context.getLeader()).send(context.topic(), request);
    }
  }

  /**
   * Cancels the status timer.
   */
  private void cancelStatusTimer() {
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling status timer", context.cluster().member().id());
      currentTimer.cancel(false);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelStatusTimer);
  }

}
