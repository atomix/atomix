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
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.internal.util.concurrent.Futures;
import net.kuujo.copycat.internal.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.*;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Observable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

/**
 * Copycat state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatStateContext extends Observable implements RaftProtocol {
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("copycat-context-%d"));
  private final Log log;
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
  private final Map<String, MemberInfo> members = new ConcurrentHashMap<>();
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

  public CopycatStateContext(String uri, ClusterConfig config, Log log) {
    this.localMember = uri;
    for (String member : config.getMembers()) {
      this.members.put(member, new MemberInfo(member, Member.Type.MEMBER, Member.State.ALIVE));
    }
    this.members.put(uri, new MemberInfo(uri, config.getMembers().contains(uri) ? Member.Type.MEMBER : Member.Type.LISTENER, Member.State.ALIVE));
    this.log = log;
    this.electionTimeout = config.getElectionTimeout();
    this.heartbeatInterval = config.getHeartbeatInterval();
  }

  /**
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  public MemberInfo getLocalMember() {
    return members.get(localMember);
  }

  /**
   * Returns a set of all members in the state cluster.
   *
   * @return A set of all members in the state cluster.
   */
  public Collection<MemberInfo> getMembers() {
    return members.values();
  }

  /**
   * Sets all members in the state cluster.
   *
   * @param members A collection of all members in the state cluster.
   * @return The Copycat state context.
   */
  public CopycatStateContext setMembers(Collection<MemberInfo> members) {
    Assert.isNotNull(members, "members");
    for (MemberInfo member : members) {
      MemberInfo record = this.members.get(member.uri());
      if (record != null) {
        record.update(member);
      } else {
        this.members.put(member.uri(), member);
      }
    }
    getLocalMember().succeed();
    triggerChangeEvent();
    return this;
  }

  /**
   * Returns a member in the state cluster.
   *
   * @param uri The member URI.
   * @return The member info.
   */
  public MemberInfo getMember(String uri) {
    return members.get(Assert.isNotNull(uri, "uri"));
  }

  /**
   * Sets a single member in the state cluster.
   *
   * @param member The member to set.
   * @return The Copycat state context.
   */
  public CopycatStateContext addMember(MemberInfo member) {
    MemberInfo record = members.get(member.uri());
    if (record != null) {
      record.update(member);
    } else {
      this.members.put(member.uri(), member);
      triggerChangeEvent();
    }
    return this;
  }

  /**
   * Removes a member in the state cluster.
   *
   * @param member The member to remove.
   * @return The Copycat state context.
   */
  public CopycatStateContext removeMember(MemberInfo member) {
    this.members.remove(member.uri());
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
  public CopycatStateContext setLeader(String leader) {
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
  public CopycatStateContext setVersion(long version) {
    this.version = Math.max(this.version, version);
    getLocalMember().update(MemberInfo.builder(getLocalMember()).withVersion(this.version).build());
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
  public CopycatStateContext setCommitIndex(Long commitIndex) {
    this.commitIndex = Assert.arg(Assert.isNotNull(commitIndex, "commitIndex"), commitIndex >= this.commitIndex, "cannot decrease commit index");
    getLocalMember().update(MemberInfo.builder(getLocalMember()).withIndex(commitIndex).build());
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
  public CopycatStateContext setLastApplied(Long lastApplied) {
    this.lastApplied = Assert.arg(Assert.isNotNull(lastApplied, "lastApplied"), lastApplied >= this.lastApplied, "cannot decrease last applied index");
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
  public CopycatStateContext setElectionTimeout(long electionTimeout) {
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
  public CopycatStateContext setHeartbeatInterval(long heartbeatInterval) {
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
  public Log log() {
    return log;
  }

  @Override
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    return state.sync(request);
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
    return state.ping(request);
  }

  @Override
  public CopycatStateContext pollHandler(MessageHandler<PollRequest, PollResponse> handler) {
    this.pollHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return state.poll(request);
  }

  @Override
  public CopycatStateContext appendHandler(MessageHandler<AppendRequest, AppendResponse> handler) {
    this.appendHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    return state.append(request);
  }

  @Override
  public CopycatStateContext queryHandler(MessageHandler<QueryRequest, QueryResponse> handler) {
    this.queryHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    return state.query(request);
  }

  @Override
  public CopycatStateContext commitHandler(MessageHandler<CommitRequest, CommitResponse> handler) {
    this.commitHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<CommitResponse> commit(CommitRequest request) {
    return state.commit(request);
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
  public CompletableFuture<Void> open() {
    if (openFuture != null) {
      return openFuture;
    }
    openFuture = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        open = true;
        log.open();
        transition(getLocalMember().type() == Member.Type.LISTENER ? CopycatState.PASSIVE : CopycatState.FOLLOWER);
      } catch (Exception e) {
        openFuture.completeExceptionally(e);
        openFuture = null;
      }
    });
    return openFuture;
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
  public String toString() {
    return getClass().getCanonicalName();
  }

}
