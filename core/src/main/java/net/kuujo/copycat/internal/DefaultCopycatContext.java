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

import net.kuujo.copycat.*;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ClusterContext;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Default Copycat context implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycatContext implements RaftContext, CopycatContext {
  private final Log log;
  private final ClusterContext cluster;
  private final ExecutionContext executor;
  private AbstractState state;
  private MessageHandler<PingRequest, PingResponse> pingHandler;
  private MessageHandler<ConfigureRequest, ConfigureResponse> configureHandler;
  private MessageHandler<PollRequest, PollResponse> pollHandler;
  private MessageHandler<SyncRequest, SyncResponse> syncHandler;
  private MessageHandler<CommitRequest, CommitResponse> commitHandler;
  @SuppressWarnings("rawtypes")
  private EventHandler applyHandler;

  public DefaultCopycatContext(Log log, ClusterContext cluster, ExecutionContext executor) {
    this.log = log;
    this.cluster = cluster;
    this.executor = executor;
  }

  @Override
  public CopycatState state() {
    return state.state();
  }

  @Override
  public Log log() {
    return log;
  }

  @Override
  public ClusterContext cluster() {
    return cluster;
  }

  @Override
  public ExecutionContext executor() {
    return executor;
  }

  @Override
  public CompletableFuture<ClusterConfig> configure(ClusterConfig config) {
    CompletableFuture<ClusterConfig> future = new CompletableFuture<>();
    ConfigureRequest request = ConfigureRequest.builder()
      .withId(UUID.randomUUID().toString())
      .withMember(cluster.getLocalMember())
      .withMembers(config.getMembers())
      .build();
    configure(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(config);
        } else {
          future.completeExceptionally(response.error());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  @Override
  public <T, U> CompletableFuture<U> submit(T entry) {
    return submit(entry, new SubmitOptions());
  }

  @Override
  public <T, U> CompletableFuture<U> submit(T entry, SubmitOptions options) {
    CompletableFuture<U> future = new CompletableFuture<>();
    CommitRequest request = CommitRequest.builder()
      .withId(UUID.randomUUID().toString())
      .withEntry(entry)
      .withConsistent(options.isConsistent())
      .withPersistent(options.isPersistent())
      .build();
    commit(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(response.result());
        } else {
          future.completeExceptionally(response.error());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  @Override
  public RaftContext pingHandler(MessageHandler<PingRequest, PingResponse> handler) {
    this.pingHandler = handler;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<PingResponse> ping(PingRequest request) {
    return state.ping(request);
  }

  @Override
  public RaftContext pollHandler(MessageHandler<PollRequest, PollResponse> handler) {
    this.pollHandler = handler;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return state.poll(request);
  }

  @Override
  public RaftContext configureHandler(MessageHandler<ConfigureRequest, ConfigureResponse> handler) {
    this.configureHandler = handler;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
    return state.configure(request);
  }

  @Override
  public RaftContext syncHandler(MessageHandler<SyncRequest, SyncResponse> handler) {
    this.syncHandler = handler;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    return state.sync(request);
  }

  @Override
  public RaftContext commitHandler(MessageHandler<CommitRequest, CommitResponse> handler) {
    this.commitHandler = handler;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<CommitResponse> commit(CommitRequest request) {
    return state.commit(request);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public CopycatContext handler(EventHandler handler) {
    this.applyHandler = handler;
    return this;
  }

  /**
   * Transition handler.
   */
  private CompletableFuture<CopycatState> transition(CopycatState state) {
    CompletableFuture<CopycatState> future = new CompletableFuture<>();
    this.state.close().whenComplete((result, error) -> {
      unregisterHandlers(this.state);
      if (error == null) {
        switch (state) {
          case START:
            this.state = new StartState(this);
            break;
          case FOLLOWER:
            this.state = new FollowerState(this);
            break;
          case CANDIDATE:
            this.state = new CandidateState(this);
            break;
          case LEADER:
            this.state = new LeaderState(this);
            break;
          default:
            this.state = new StartState(this);
            break;
        }
        this.state.open().whenComplete((result2, error2) -> {
          if (error2 == null) {
            registerHandlers(this.state);
            future.complete(this.state.state());
          } else {
            future.completeExceptionally(error2);
          }
        });
      }
    });
    return future;
  }

  /**
   * Registers handlers on the given state.
   */
  private void registerHandlers(AbstractState state) {
    state.pingHandler(pingHandler);
    state.syncHandler(syncHandler);
    state.configureHandler(configureHandler);
    state.pollHandler(pollHandler);
    state.commitHandler(commitHandler);
    state.applyHandler(applyHandler);
    state.transitionHandler(this::transition);
  }

  /**
   * Unregisters handlers on the given state.
   */
  private void unregisterHandlers(AbstractState state) {
    state.pingHandler(null);
    state.syncHandler(null);
    state.configureHandler(null);
    state.pollHandler(null);
    state.commitHandler(null);
    state.applyHandler(null);
    state.transitionHandler(null);
  }

  @Override
  public CompletableFuture<Void> open() {
    return transition(CopycatState.START).thenApply((state) -> null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return transition(CopycatState.START).thenApply((state) -> null);
  }

}
