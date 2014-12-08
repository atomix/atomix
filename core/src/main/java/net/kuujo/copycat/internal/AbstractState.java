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
import net.kuujo.copycat.EventHandler;
import net.kuujo.copycat.RaftContext;
import net.kuujo.copycat.cluster.ClusterContext;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractState implements RaftContext {
  protected final DefaultCopycatContext context;
  private MessageHandler<PingRequest, PingResponse> pingHandler;
  private MessageHandler<ConfigureRequest, ConfigureResponse> configureHandler;
  private MessageHandler<PollRequest, PollResponse> pollHandler;
  private MessageHandler<SyncRequest, SyncResponse> syncHandler;
  private MessageHandler<CommitRequest, CommitResponse> commitHandler;
  private EventHandler applyHandler;
  private EventHandler<CopycatState, CompletableFuture<CopycatState>> transitionHandler;

  public AbstractState(DefaultCopycatContext context) {
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

  @Override
  public ClusterContext cluster() {
    return context.cluster();
  }

  @Override
  public Log log() {
    return context.log();
  }

  @Override
  public ExecutionContext executor() {
    return context.executor();
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
  public AbstractState configureHandler(MessageHandler<ConfigureRequest, ConfigureResponse> handler) {
    this.configureHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  @Override
  public AbstractState syncHandler(MessageHandler<SyncRequest, SyncResponse> handler) {
    this.syncHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
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

  public AbstractState applyHandler(EventHandler handler) {
    this.applyHandler = handler;
    return this;
  }

  /**
   * Sets a transition handler on the state.
   */
  public AbstractState transitionHandler(EventHandler<CopycatState, CompletableFuture<CopycatState>> handler) {
    this.transitionHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<Void> open() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

}
