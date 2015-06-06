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
  protected final RaftContext context;
  private volatile boolean open;

  protected AbstractState(RaftContext context) {
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
    context.getCluster().member().<RegisterRequest, RegisterResponse>registerHandler(RegisterRequest.class, this::register);
    context.getCluster().member().<KeepAliveRequest, KeepAliveResponse>registerHandler(KeepAliveRequest.class, this::keepAlive);
    context.getCluster().member().<AppendRequest, AppendResponse>registerHandler(AppendRequest.class, this::append);
    context.getCluster().member().<PollRequest, PollResponse>registerHandler(PollRequest.class, this::poll);
    context.getCluster().member().<VoteRequest, VoteResponse>registerHandler(VoteRequest.class, this::vote);
    context.getCluster().member().<CommandRequest, CommandResponse>registerHandler(CommandRequest.class, this::command);
    context.getCluster().member().<QueryRequest, QueryResponse>registerHandler(QueryRequest.class, this::query);
  }

  /**
   * Unregisters all message handlers.
   */
  private void unregisterHandlers() {
    context.checkThread();
    context.getCluster().member().unregisterHandler(RegisterRequest.class);
    context.getCluster().member().unregisterHandler(KeepAliveRequest.class);
    context.getCluster().member().unregisterHandler(AppendRequest.class);
    context.getCluster().member().unregisterHandler(PollRequest.class);
    context.getCluster().member().unregisterHandler(VoteRequest.class);
    context.getCluster().member().unregisterHandler(CommandRequest.class);
    context.getCluster().member().unregisterHandler(QueryRequest.class);
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
