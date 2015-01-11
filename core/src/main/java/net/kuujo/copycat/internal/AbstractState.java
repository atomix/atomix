/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.protocol.*;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class AbstractState implements RaftProtocol {
  protected final CopycatStateContext context;
  protected MessageHandler<SyncRequest, SyncResponse> syncHandler;
  protected MessageHandler<PingRequest, PingResponse> pingHandler;
  protected MessageHandler<PollRequest, PollResponse> pollHandler;
  protected MessageHandler<AppendRequest, AppendResponse> appendHandler;
  protected MessageHandler<CommitRequest, CommitResponse> commitHandler;
  protected MessageHandler<QueryRequest, QueryResponse> queryHandler;
  protected MessageHandler<CopycatState, CopycatState> transitionHandler;
  private boolean open;

  protected AbstractState(CopycatStateContext context) {
    this.context = context;
  }

  /**
   * Returns an exceptional future with the given exception.
   */
  protected <T> CompletableFuture<T> exceptionalFuture(Throwable t) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(t);
    return future;
  }

  /**
   * Returns the Copycat state represented by this state.
   *
   * @return The Copycat state represented by this state.
   */
  public abstract CopycatState state();

  /**
   * Returns the state logger.
   */
  protected abstract Logger logger();

  /**
   * Logs a request.
   */
  protected final <R extends Request> R logRequest(R request) {
    logger().debug("{} - Received {}", context.getLocalMember(), request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends Response> R logResponse(R response) {
    logger().debug("{} - Sent {}", context.getLocalMember(), response);
    return response;
  }

  @Override
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  @Override
  public RaftProtocol syncHandler(MessageHandler<SyncRequest, SyncResponse> handler) {
    this.syncHandler = handler;
    return this;
  }

  @Override
  public AbstractState pingHandler(MessageHandler<PingRequest, PingResponse> handler) {
    this.pingHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<PingResponse> ping(PingRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  @Override
  public AbstractState pollHandler(MessageHandler<PollRequest, PollResponse> handler) {
    this.pollHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  @Override
  public AbstractState appendHandler(MessageHandler<AppendRequest, AppendResponse> handler) {
    this.appendHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  @Override
  public RaftProtocol queryHandler(MessageHandler<QueryRequest, QueryResponse> handler) {
    this.queryHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  @Override
  public AbstractState commitHandler(MessageHandler<CommitRequest, CommitResponse> handler) {
    this.commitHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<CommitResponse> commit(CommitRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  /**
   * Sets a transition registerHandler on the state.
   */
  public AbstractState transitionHandler(MessageHandler<CopycatState, CopycatState> handler) {
    this.transitionHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<Void> open() {
    context.checkThread();
    open = true;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

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

}
