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

import net.kuujo.copycat.raft.protocol.request.*;
import net.kuujo.copycat.raft.protocol.response.*;
import net.kuujo.copycat.raft.RaftServer;
import net.kuujo.copycat.util.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class AbstractState implements Managed<AbstractState> {
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final ServerContext context;
  private volatile boolean open;

  protected AbstractState(ServerContext context) {
    this.context = context;
  }

  /**
   * Returns the Copycat state represented by this state.
   *
   * @return The Copycat state represented by this state.
   */
  public abstract RaftServer.State type();

  /**
   * Logs a request.
   */
  protected final <R extends Request> R logRequest(R request) {
    LOGGER.debug("{} - Received {}", context.getMember().id(), request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends Response> R logResponse(R response) {
    LOGGER.debug("{} - Sent {}", context.getMember().id(), response);
    return response;
  }

  @Override
  public CompletableFuture<AbstractState> open() {
    context.checkThread();
    open = true;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  /**
   * Transitions to a new state.
   */
  protected void transition(RaftServer.State state) {
    if (state == type())
      return;

    switch (state) {
      case INACTIVE:
        context.transition(InactiveState.class);
        break;
      case PASSIVE:
        context.transition(PassiveState.class);
        break;
      case FOLLOWER:
        context.transition(FollowerState.class);
        break;
      case CANDIDATE:
        context.transition(CandidateState.class);
        break;
      case LEADER:
        context.transition(LeaderState.class);
        break;
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Handles a register request.
   */
  protected abstract CompletableFuture<RegisterResponse> register(RegisterRequest request);

  /**
   * Handles a keep alive request.
   */
  protected abstract CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request);

  /**
   * Handles a join request.
   */
  protected abstract CompletableFuture<JoinResponse> join(JoinRequest request);

  /**
   * Handles a leave request.
   */
  protected abstract CompletableFuture<LeaveResponse> leave(LeaveRequest request);

  /**
   * Handles an append request.
   */
  protected abstract CompletableFuture<AppendResponse> append(AppendRequest request);

  /**
   * Handles a poll request.
   */
  protected abstract CompletableFuture<PollResponse> poll(PollRequest request);

  /**
   * Handles a vote request.
   */
  protected abstract CompletableFuture<VoteResponse> vote(VoteRequest request);

  /**
   * Handles a command request.
   */
  protected abstract CompletableFuture<CommandResponse> command(CommandRequest request);

  /**
   * Handles a query request.
   */
  protected abstract CompletableFuture<QueryResponse> query(QueryRequest request);

  @Override
  public CompletableFuture<Void> close() {
    context.checkThread();
    open = false;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public String toString() {
    return String.format("%s[context=%s]", getClass().getSimpleName(), context);
  }

}
