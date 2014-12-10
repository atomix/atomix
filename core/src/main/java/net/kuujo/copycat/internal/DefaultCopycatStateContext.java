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

import net.kuujo.copycat.Action;
import net.kuujo.copycat.ActionOptions;
import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultCopycatStateContext extends Observable implements CopycatContext, CopycatStateContext {
  private final Cluster cluster;
  private final ExecutionContext executor;
  private final Log log;
  private AbstractState state;
  private MessageHandler<PingRequest, PingResponse> pingHandler;
  private MessageHandler<ConfigureRequest, ConfigureResponse> configureHandler;
  private MessageHandler<PollRequest, PollResponse> pollHandler;
  private MessageHandler<SyncRequest, SyncResponse> syncHandler;
  private MessageHandler<CommitRequest, CommitResponse> commitHandler;
  private final Map<String, ActionInfo> actions = new HashMap<>();
  private final String localMember;
  private final Set<String> remoteMembers;
  private Election.Status status;
  private String leader;
  private long term;
  private String lastVotedFor;
  private long commitIndex;
  private long lastApplied;
  private long electionTimeout = 500;
  private long heartbeatInterval = 250;

  public DefaultCopycatStateContext(Cluster cluster, ClusterConfig config, Log log, ExecutionContext executor) {
    this.cluster = cluster;
    this.localMember = config.getLocalMember();
    this.remoteMembers = config.getRemoteMembers();
    this.log = log;
    this.executor = executor;
    this.electionTimeout = config.getElectionTimeout();
    this.heartbeatInterval = config.getHeartbeatInterval();
  }

  @Override
  public Cluster cluster() {
    return cluster;
  }

  @Override
  public String getLocalMember() {
    return localMember;
  }

  @Override
  public CopycatStateContext addMember(String uri) {
    if (!localMember.equals(uri) && remoteMembers.add(uri)) {
      triggerChangeEvent();
    }
    return this;
  }

  @Override
  public CopycatStateContext removeMember(String uri) {
    if (!localMember.equals(uri) && remoteMembers.remove(uri)) {
      triggerChangeEvent();
    }
    return this;
  }

  @Override
  public CopycatStateContext setMembers(Set<String> members) {
    members.remove(localMember);
    remoteMembers.clear();
    remoteMembers.addAll(members);
    return this;
  }

  @Override
  public Set<String> getMembers() {
    Set<String> members = new HashSet<>(remoteMembers);
    members.add(localMember);
    return members;
  }

  @Override
  public Set<String> getRemoteMembers() {
    return new HashSet<>(remoteMembers);
  }

  @Override
  public Election.Status getStatus() {
    return status;
  }

  @Override
  public CopycatStateContext setLeader(String leader) {
    if (this.leader == null) {
      if (leader != null) {
        this.leader = leader;
        this.lastVotedFor = null;
        this.status = Election.Status.COMPLETE;
        triggerChangeEvent();
      }
    } else if (leader != null) {
      if (!this.leader.equals(leader)) {
        this.leader = leader;
        this.lastVotedFor = null;
        this.status = Election.Status.COMPLETE;
        triggerChangeEvent();
      }
    } else {
      this.leader = null;
      this.status = Election.Status.IN_PROGRESS;
      triggerChangeEvent();
    }
    return this;
  }

  @Override
  public String getLeader() {
    return leader;
  }

  @Override
  public CopycatStateContext setTerm(long term) {
    if (term > this.term) {
      this.term = term;
      this.leader = null;
      this.status = Election.Status.IN_PROGRESS;
      this.lastVotedFor = null;
      triggerChangeEvent();
    }
    return this;
  }

  @Override
  public long getTerm() {
    return term;
  }

  @Override
  public CopycatStateContext setLastVotedFor(String candidate) {
    // If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
    if (lastVotedFor != null && candidate != null) {
      throw new IllegalStateException("Already voted for another candidate");
    }
    if (leader != null && candidate != null) {
      throw new IllegalStateException("Cannot cast vote - leader already exists");
    }
    this.lastVotedFor = candidate;
    this.status = Election.Status.IN_PROGRESS;
    triggerChangeEvent();
    return this;
  }

  @Override
  public String getLastVotedFor() {
    return lastVotedFor;
  }

  @Override
  public CopycatStateContext setCommitIndex(long commitIndex) {
    if (commitIndex <= this.commitIndex) {
      throw new IllegalStateException("Cannot decrease commit index");
    }
    this.commitIndex = commitIndex;
    triggerChangeEvent();
    return this;
  }

  @Override
  public long getCommitIndex() {
    return commitIndex;
  }

  @Override
  public CopycatStateContext setLastApplied(long lastApplied) {
    if (lastApplied <= this.lastApplied) {
      throw new IllegalStateException("Cannot decrease last applied index");
    }
    this.lastApplied = lastApplied;
    triggerChangeEvent();
    return this;
  }

  @Override
  public long getLastApplied() {
    return lastApplied;
  }

  @Override
  public CopycatStateContext setElectionTimeout(long electionTimeout) {
    this.electionTimeout = electionTimeout;
    return this;
  }

  @Override
  public long getElectionTimeout() {
    return electionTimeout;
  }

  @Override
  public CopycatStateContext setHeartbeatInterval(long heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
    return this;
  }

  @Override
  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  @Override
  public CopycatState state() {
    return state.state();
  }

  @Override
  public ExecutionContext executor() {
    return executor;
  }

  @Override
  public Log log() {
    return log;
  }

  @Override
  public CompletableFuture<ClusterConfig> configure(ClusterConfig config) {
    CompletableFuture<ClusterConfig> future = new CompletableFuture<>();
    ConfigureRequest request = ConfigureRequest.builder()
      .withId(UUID.randomUUID().toString())
      .withMember(localMember)
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
  public <T, U> CopycatContext register(String name, Action<T, U> action) {
    actions.put(name, new ActionInfo(name, action, new ActionOptions()));
    return this;
  }

  @Override
  public <T, U> CopycatContext register(String name, Action<T, U> action, ActionOptions options) {
    actions.put(name, new ActionInfo(name, action, options));
    return this;
  }

  @Override
  public CopycatContext unregister(String name) {
    actions.remove(name);
    return this;
  }

  /**
   * Returns action info for an action.
   *
   * @param name The action name for which to return action info.
   * @return The action info.
   */
  ActionInfo action(String name) {
    return actions.get(name);
  }

  @Override
  public <T, U> CompletableFuture<U> submit(String action, T entry) {
    CompletableFuture<U> future = new CompletableFuture<>();
    CommitRequest request = CommitRequest.builder()
      .withId(UUID.randomUUID().toString())
      .withMember(getLocalMember())
      .withEntry(entry)
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
  public DefaultCopycatStateContext pingHandler(MessageHandler<PingRequest, PingResponse> handler) {
    this.pingHandler = handler;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<PingResponse> ping(PingRequest request) {
    return state.ping(request);
  }

  @Override
  public DefaultCopycatStateContext pollHandler(MessageHandler<PollRequest, PollResponse> handler) {
    this.pollHandler = handler;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return state.poll(request);
  }

  @Override
  public DefaultCopycatStateContext configureHandler(MessageHandler<ConfigureRequest, ConfigureResponse> handler) {
    this.configureHandler = handler;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
    return state.configure(request);
  }

  @Override
  public DefaultCopycatStateContext syncHandler(MessageHandler<SyncRequest, SyncResponse> handler) {
    this.syncHandler = handler;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    return state.sync(request);
  }

  @Override
  public DefaultCopycatStateContext commitHandler(MessageHandler<CommitRequest, CommitResponse> handler) {
    this.commitHandler = handler;
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<CommitResponse> commit(CommitRequest request) {
    return state.commit(request);
  }

  /**
   * Transition handler.
   */
  private CompletableFuture<CopycatState> transition(CopycatState state) {
    if (state == this.state.state()) {
      return CompletableFuture.completedFuture(this.state.state());
    }

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
    state.transitionHandler(null);
  }

  /**
   * Triggers an observable changed event.
   */
  private void triggerChangeEvent() {
    setChanged();
    notifyObservers();
    clearChanged();
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
