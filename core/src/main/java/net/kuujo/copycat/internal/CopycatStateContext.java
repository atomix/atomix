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
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.cluster.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.concurrent.Futures;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.log.LogManager;
import net.kuujo.copycat.protocol.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Copycat state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatStateContext extends Observable implements RaftProtocol {
  private final ScheduledExecutorService executor;
  private final LogManager log;
  private AbstractState state;
  private BiFunction<Long, ByteBuffer, ByteBuffer> consumer;
  private MessageHandler<SyncRequest, SyncResponse> syncHandler;
  private MessageHandler<PingRequest, PingResponse> pingHandler;
  private MessageHandler<PollRequest, PollResponse> pollHandler;
  private MessageHandler<AppendRequest, AppendResponse> appendHandler;
  private MessageHandler<QueryRequest, QueryResponse> queryHandler;
  private MessageHandler<CommitRequest, CommitResponse> commitHandler;
  private CompletableFuture<Void> openFuture;
  private final String localMember;
  private final Set<String> replicas;
  private Set<String> members;
  private final ReplicaInfo localMemberInfo;
  private final Map<String, ReplicaInfo> memberInfo = new HashMap<>();
  private Election.Status status;
  private String leader;
  private long term;
  private long version;
  private String lastVotedFor;
  private Long commitIndex;
  private Long lastApplied;
  private long electionTimeout = 500;
  private long heartbeatInterval = 250;
  private boolean open;

  public CopycatStateContext(String name, String uri, CoordinatedResourceConfig config) {
    this.executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-context-" + name + "-%d"));
    this.localMember = Assert.isNotNull(uri, "uri");
    this.replicas = new HashSet<>(config.getReplicas());
    this.members = new HashSet<>(config.getReplicas());
    this.members.add(uri);
    this.localMemberInfo = new ReplicaInfo(uri);
    this.memberInfo.put(uri, localMemberInfo);
    this.log = config.getLog().getLogManager(name);
    this.electionTimeout = config.getElectionTimeout();
    this.heartbeatInterval = config.getHeartbeatInterval();
  }

  /**
   * Returns the full set of replicas.
   *
   * @return The full set of Raft replicas.
   */
  public Set<String> getReplicas() {
    return replicas;
  }

  /**
   * Returns the local member URI.
   *
   * @return The local member URI.
   */
  public String getLocalMember() {
    return localMember;
  }

  /**
   * Returns the full set of Raft members.
   *
   * @return The full set of Raft members.
   */
  public Set<String> getMembers() {
    return members;
  }

  /**
   * Sets the full set of Raft members.
   *
   * @param members The full set of Raft members.
   * @return The Copycat state context.
   */
  CopycatStateContext setMembers(Collection<String> members) {
    this.members = new HashSet<>(members);
    return this;
  }

  /**
   * Adds a member to the state context.
   *
   * @param member The member URI to add.
   * @return The Copycat state context.
   */
  CopycatStateContext addMember(String member) {
    this.members.add(member);
    return this;
  }

  /**
   * Removes a member from the state context.
   *
   * @param member The member URI to remove.
   * @return The Copycat state context.
   */
  CopycatStateContext removeMember(String member) {
    this.members.remove(member);
    return this;
  }

  /**
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  public ReplicaInfo getLocalMemberInfo() {
    return localMemberInfo;
  }

  /**
   * Returns a set of all members in the state cluster.
   *
   * @return A set of all members in the state cluster.
   */
  public Collection<ReplicaInfo> getMemberInfo() {
    return memberInfo.values().stream().filter(info -> members.contains(info.getUri())).collect(Collectors.toList());
  }

  /**
   * Sets all members info in the state cluster.
   *
   * @param members A collection of all members in the state cluster.
   * @return The Copycat state context.
   */
  CopycatStateContext setMemberInfo(Collection<ReplicaInfo> members) {
    Assert.isNotNull(members, "members");
    for (ReplicaInfo member : members) {
      ReplicaInfo record = this.memberInfo.get(member.getUri());
      if (record != null) {
        record.update(member);
      } else {
        this.memberInfo.put(member.getUri(), member);
      }
    }
    triggerChangeEvent();
    return this;
  }

  /**
   * Returns a member info in the state cluster.
   *
   * @param uri The member URI.
   * @return The member info.
   */
  public ReplicaInfo getMemberInfo(String uri) {
    return memberInfo.get(Assert.isNotNull(uri, "uri"));
  }

  /**
   * Sets a single member info in the state cluster.
   *
   * @param member The member to set.
   * @return The Copycat state context.
   */
  CopycatStateContext addMemberInfo(ReplicaInfo member) {
    ReplicaInfo record = memberInfo.get(member.getUri());
    if (record != null) {
      record.update(member);
    } else {
      this.memberInfo.put(member.getUri(), member);
      triggerChangeEvent();
    }
    return this;
  }

  /**
   * Removes a member info in the state cluster.
   *
   * @param member The member to remove.
   * @return The Copycat state context.
   */
  CopycatStateContext removeMemberInfo(ReplicaInfo member) {
    this.members.remove(member.getUri());
    triggerChangeEvent();
    return this;
  }

  /**
   * Returns the current Copycat election status.
   *
   * @return The current Copycat election status.
   */
  public Election.Status getStatus() {
    return status;
  }

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   * @return The Copycat state context.
   */
  CopycatStateContext setLeader(String leader) {
    if (this.leader == null) {
      if (leader != null) {
        this.leader = leader;
        this.lastVotedFor = null;
        this.status = Election.Status.COMPLETE;
        if (openFuture != null) {
          openFuture.complete(null);
        }
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

  /**
   * Returns the state leader.
   *
   * @return The state leader.
   */
  public String getLeader() {
    return leader;
  }

  /**
   * Sets the state term.
   *
   * @param term The state term.
   * @return The Copycat state context.
   */
  CopycatStateContext setTerm(long term) {
    if (term > this.term) {
      this.term = term;
      this.leader = null;
      this.status = Election.Status.IN_PROGRESS;
      this.lastVotedFor = null;
      triggerChangeEvent();
    }
    return this;
  }

  /**
   * Returns the state term.
   *
   * @return The state term.
   */
  public long getTerm() {
    return term;
  }

  /**
   * Sets the state version.
   *
   * @param version The state version.
   * @return The Copycat state context.
   */
  CopycatStateContext setVersion(long version) {
    this.version = Math.max(this.version, version);
    localMemberInfo.setVersion(this.version);
    return this;
  }

  /**
   * Returns the state version.
   *
   * @return The state version.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Sets the state last voted for candidate.
   *
   * @param candidate The candidate that was voted for.
   * @return The Copycat state context.
   */
  CopycatStateContext setLastVotedFor(String candidate) {
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

  /**
   * Returns the state last voted for candidate.
   *
   * @return The state last voted for candidate.
   */
  public String getLastVotedFor() {
    return lastVotedFor;
  }

  /**
   * Sets the state commit index.
   *
   * @param commitIndex The state commit index.
   * @return The Copycat state context.
   */
  CopycatStateContext setCommitIndex(Long commitIndex) {
    this.commitIndex = this.commitIndex != null ? Assert.arg(Assert.isNotNull(commitIndex, "commitIndex"), commitIndex >= this.commitIndex, "cannot decrease commit index") : commitIndex;
    localMemberInfo.setIndex(this.commitIndex);
    return this;
  }

  /**
   * Returns the state commit index.
   *
   * @return The state commit index.
   */
  public Long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Sets the state last applied index.
   *
   * @param lastApplied The state last applied index.
   * @return The Copycat state context.
   */
  CopycatStateContext setLastApplied(Long lastApplied) {
    this.lastApplied = this.lastApplied != null ? Assert.arg(Assert.isNotNull(lastApplied, "lastApplied"), lastApplied >= this.lastApplied, "cannot decrease last applied index") : lastApplied;
    return this;
  }

  /**
   * Returns the state last applied index.
   *
   * @return The state last applied index.
   */
  public Long getLastApplied() {
    return lastApplied;
  }

  /**
   * Sets the state election timeout.
   *
   * @param electionTimeout The state election timeout.
   * @return The Copycat state context.
   */
  CopycatStateContext setElectionTimeout(long electionTimeout) {
    this.electionTimeout = electionTimeout;
    return this;
  }

  /**
   * Returns the state election timeout.
   *
   * @return The state election timeout.
   */
  public long getElectionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the state heartbeat interval.
   *
   * @param heartbeatInterval The state heartbeat interval.
   * @return The Copycat state context.
   */
  CopycatStateContext setHeartbeatInterval(long heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
    return this;
  }

  /**
   * Returns the state heartbeat interval.
   *
   * @return The state heartbeat interval.
   */
  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Returns the Copycat state.
   *
   * @return The current Copycat state.
   */
  public CopycatState state() {
    return state.state();
  }

  /**
   * Returns the context executor.
   *
   * @return The context executor.
   */
  public ScheduledExecutorService executor() {
    return executor;
  }

  /**
   * Registers an entry consumer on the context.
   *
   * @param consumer The entry consumer.
   * @return The Copycat context.
   */
  public CopycatStateContext consumer(BiFunction<Long, ByteBuffer, ByteBuffer> consumer) {
    this.consumer = consumer;
    return this;
  }

  /**
   * Returns the log consumer.
   *
   * @return The log consumer.
   */
  public BiFunction<Long, ByteBuffer, ByteBuffer> consumer() {
    return consumer;
  }

  /**
   * Returns the state log.
   *
   * @return The state log.
   */
  public LogManager log() {
    return log;
  }

  @Override
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    return wrapCall(request, state::sync);
  }

  @Override
  public RaftProtocol syncHandler(MessageHandler<SyncRequest, SyncResponse> handler) {
    this.syncHandler = handler;
    return this;
  }

  @Override
  public CopycatStateContext pingHandler(MessageHandler<PingRequest, PingResponse> handler) {
    this.pingHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<PingResponse> ping(PingRequest request) {
    return wrapCall(request, state::ping);
  }

  @Override
  public CopycatStateContext pollHandler(MessageHandler<PollRequest, PollResponse> handler) {
    this.pollHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return wrapCall(request, state::poll);
  }

  @Override
  public CopycatStateContext appendHandler(MessageHandler<AppendRequest, AppendResponse> handler) {
    this.appendHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    return wrapCall(request, state::append);
  }

  @Override
  public CopycatStateContext queryHandler(MessageHandler<QueryRequest, QueryResponse> handler) {
    this.queryHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    return wrapCall(request, state::query);
  }

  @Override
  public CopycatStateContext commitHandler(MessageHandler<CommitRequest, CommitResponse> handler) {
    this.commitHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<CommitResponse> commit(CommitRequest request) {
    return wrapCall(request, state::commit);
  }

  private <T extends Request, U extends Response> CompletableFuture<U> wrapCall(T request, MessageHandler<T, U> handler) {
    CompletableFuture<U> future = new CompletableFuture<>();
    executor.execute(() -> {
      handler.handle(request).whenComplete((response, error) -> {
        if (error == null) {
          future.complete(response);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return future;
  }

  /**
   * Transition registerHandler.
   */
  CompletableFuture<CopycatState> transition(CopycatState state) {
    if (this.state != null && state == this.state.state()) {
      return CompletableFuture.completedFuture(this.state.state());
    }

    CompletableFuture<CopycatState> future = new CompletableFuture<>();
    if (this.state != null) {
      this.state.close().whenComplete((result, error) -> {
        unregisterHandlers(this.state);
        if (error == null) {
          switch (state) {
            case START:
              this.state = new StartState(this);
              break;
            case PASSIVE:
              this.state = new PassiveState(this);
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

          registerHandlers(this.state);
          this.state.open().whenComplete((result2, error2) -> {
            if (error2 == null) {
              future.complete(this.state.state());
            } else {
              future.completeExceptionally(error2);
            }
          });
        }
      });
    } else {
      switch (state) {
        case START:
          this.state = new StartState(this);
          break;
        case PASSIVE:
          this.state = new PassiveState(this);
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

      registerHandlers(this.state);
      this.state.open().whenComplete((result, error) -> {
        if (error == null) {
          future.complete(this.state.state());
        } else {
          future.completeExceptionally(error);
        }
      });
    }
    return future;
  }

  /**
   * Registers handlers on the given state.
   */
  private void registerHandlers(AbstractState state) {
    state.syncHandler(syncHandler);
    state.pingHandler(pingHandler);
    state.appendHandler(appendHandler);
    state.pollHandler(pollHandler);
    state.queryHandler(queryHandler);
    state.commitHandler(commitHandler);
    state.transitionHandler(this::transition);
  }

  /**
   * Unregisters handlers on the given state.
   */
  private void unregisterHandlers(AbstractState state) {
    state.syncHandler(null);
    state.pingHandler(null);
    state.appendHandler(null);
    state.pollHandler(null);
    state.queryHandler(null);
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
  public synchronized CompletableFuture<Void> open() {
    if (openFuture != null) {
      return openFuture;
    }
    openFuture = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        open = true;
        log.open();
        transition(replicas.contains(localMember) ? CopycatState.FOLLOWER : CopycatState.PASSIVE);
      } catch (Exception e) {
        openFuture.completeExceptionally(e);
        openFuture = null;
      }
    });
    return openFuture;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (openFuture != null) {
      openFuture.cancel(false);
      openFuture = null;
    } else if (!open) {
      return Futures.exceptionalFuture(new IllegalStateException("Context not open"));
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    executor.execute(() -> {
      transition(CopycatState.START).whenComplete((result, error) -> {
        if (error == null) {
          try {
            log.close();
            future.complete(null);
          } catch (Exception e) {
            future.completeExceptionally(e);
          }
        } else {
          try {
            log.close();
            future.completeExceptionally(error);
          } catch (Exception e) {
            future.completeExceptionally(error);
          }
        }
      });
    });
    return future;
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public String toString() {
    return getClass().getCanonicalName();
  }

}
