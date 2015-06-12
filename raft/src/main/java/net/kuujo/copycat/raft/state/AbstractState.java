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

import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.raft.rpc.*;
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
  protected final RaftStateContext context;
  private volatile boolean open;

  protected AbstractState(RaftStateContext context) {
    this.context = context;
  }

  /**
   * Returns the Copycat state represented by this state.
   *
   * @return The Copycat state represented by this state.
   */
  public abstract RaftState type();

  /**
   * Logs a request.
   */
  protected final <R extends Request> R logRequest(R request) {
    LOGGER.debug("{} - Received {}", context.getCluster().member().id(), request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends Response> R logResponse(R response) {
    LOGGER.debug("{} - Sent {}", context.getCluster().member().id(), response);
    return response;
  }

  @Override
  public CompletableFuture<AbstractState> open() {
    context.checkThread();
    registerHandlers();
    open = true;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  /**
   * Registers all message handlers.
   */
  private void registerHandlers() {
    context.checkThread();
    context.getCluster().member().registerHandler(JoinRequest.class, wrapRequestHandler(this::join));
    context.getCluster().member().registerHandler(LeaveRequest.class, wrapRequestHandler(this::leave));
    context.getCluster().member().registerHandler(HeartbeatRequest.class, wrapRequestHandler(this::heartbeat));
    context.getCluster().member().registerHandler(RegisterRequest.class, wrapRequestHandler(this::register));
    context.getCluster().member().registerHandler(KeepAliveRequest.class, wrapRequestHandler(this::keepAlive));
    context.getCluster().member().registerHandler(AppendRequest.class, wrapRequestHandler(this::append));
    context.getCluster().member().registerHandler(PollRequest.class, wrapRequestHandler(this::poll));
    context.getCluster().member().registerHandler(VoteRequest.class, wrapRequestHandler(this::vote));
    context.getCluster().member().registerHandler(CommandRequest.class, wrapRequestHandler(this::command));
    context.getCluster().member().registerHandler(QueryRequest.class, wrapRequestHandler(this::query));
  }

  /**
   * Unregisters all message handlers.
   */
  private void unregisterHandlers() {
    context.checkThread();
    context.getCluster().member().unregisterHandler(JoinRequest.class);
    context.getCluster().member().unregisterHandler(LeaveRequest.class);
    context.getCluster().member().unregisterHandler(HeartbeatRequest.class);
    context.getCluster().member().unregisterHandler(RegisterRequest.class);
    context.getCluster().member().unregisterHandler(KeepAliveRequest.class);
    context.getCluster().member().unregisterHandler(AppendRequest.class);
    context.getCluster().member().unregisterHandler(PollRequest.class);
    context.getCluster().member().unregisterHandler(VoteRequest.class);
    context.getCluster().member().unregisterHandler(CommandRequest.class);
    context.getCluster().member().unregisterHandler(QueryRequest.class);
  }

  /**
   * Wraps a request handler.
   */
  private <T extends Request, U extends Response> MessageHandler<T, U> wrapRequestHandler(MessageHandler<T, U> handler) {
    return request -> handler.handle(request).whenComplete((result, error) -> request.close());
  }

  /**
   * Handles a join request.
   */
  protected abstract CompletableFuture<JoinResponse> join(JoinRequest request);

  /**
   * Handles a leave request.
   */
  protected abstract CompletableFuture<LeaveResponse> leave(LeaveRequest request);

  /**
   * Handles a heartbeat request.
   */
  protected abstract CompletableFuture<HeartbeatResponse> heartbeat(HeartbeatRequest request);

  /**
   * Handles a register request.
   */
  protected abstract CompletableFuture<RegisterResponse> register(RegisterRequest request);

  /**
   * Handles a keep alive request.
   */
  protected abstract CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request);

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
    unregisterHandlers();
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
