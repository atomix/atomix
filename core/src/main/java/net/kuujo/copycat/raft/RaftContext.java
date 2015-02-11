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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.log.LogManager;
import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.util.concurrent.Futures;
import net.kuujo.copycat.util.function.TriFunction;
import net.kuujo.copycat.util.internal.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftContext extends Observable implements RaftProtocol {
  private final Logger LOGGER = LoggerFactory.getLogger(RaftContext.class);
  private final ScheduledExecutorService executor;
  private Thread thread;
  private final RaftConfig config;
  private final LogManager log;
  private RaftState state;
  private TriFunction<Long, Long, ByteBuffer, ByteBuffer> consumer;
  private MessageHandler<JoinRequest, JoinResponse> joinHandler;
  private MessageHandler<PromoteRequest, PromoteResponse> promoteHandler;
  private MessageHandler<LeaveRequest, LeaveResponse> leaveHandler;
  private MessageHandler<SyncRequest, SyncResponse> syncHandler;
  private MessageHandler<PollRequest, PollResponse> pollHandler;
  private MessageHandler<VoteRequest, VoteResponse> voteHandler;
  private MessageHandler<AppendRequest, AppendResponse> appendHandler;
  private MessageHandler<QueryRequest, QueryResponse> queryHandler;
  private MessageHandler<CommitRequest, CommitResponse> commitHandler;
  private CompletableFuture<Void> openFuture;
  private final RaftMember localMember;
  private final Map<String, RaftMember> members = new HashMap<>();
  private boolean recovering = true;
  private String leader;
  private long term;
  private long version;
  private String lastVotedFor;
  private Long firstCommitIndex;
  private Long commitIndex;
  private Long lastApplied;
  private long electionTimeout = 500;
  private long heartbeatInterval = 250;
  private volatile boolean open;

  public RaftContext(String name, String uri, RaftConfig config, ScheduledExecutorService executor) {
    this.executor = executor;
    this.config = config;
    this.localMember = new RaftMember(Assert.isNotNull(uri, "uri"), config.getReplicas().contains(uri) ? Member.Type.PROMOTABLE : Member.Type.PASSIVE, Member.Status.ALIVE);
    members.put(localMember.uri(), localMember);
    config.getReplicas().forEach(r -> {
      members.put(r, new RaftMember(r, Member.Type.ACTIVE, Member.Status.ALIVE));
    });
    this.log = config.getLog().getLogManager(name);
    this.electionTimeout = config.getElectionTimeout();
    this.heartbeatInterval = config.getHeartbeatInterval();
    try {
      executor.submit(() -> this.thread = Thread.currentThread()).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CopycatException(e);
    }
  }

  /**
   * Returns the Raft configuration.
   *
   * @return The Raft configuration.
   */
  public RaftConfig getConfig() {
    return config;
  }

  /**
   * Returns the local member.
   *
   * @return The local member.
   */
  public RaftMember getLocalMember() {
    return localMember;
  }

  /**
   * Returns a collection of all members in the cluster.
   *
   * @return A collection of all members in the cluster.
   */
  public Collection<RaftMember> getMembers() {
    return members.values();
  }

  /**
   * Returns member info for a specific member.
   *
   * @param uri The URI of the member for which to return member info.
   * @return The Raft member.
   */
  public RaftMember getMember(String uri) {
    return members.get(uri);
  }

  /**
   * Sets the set of active and promotable members.
   *
   * @param members A collection of active and promotable members.
   * @return The Raft context.
   */
  RaftContext setMembers(Collection<RaftMember> members) {
    Iterator<Map.Entry<String, RaftMember>> iterator = this.members.entrySet().iterator();
    while (iterator.hasNext()) {
      RaftMember member = iterator.next().getValue();
      if ((member.type() == Member.Type.ACTIVE || member.type() == Member.Type.PROMOTABLE) && !members.contains(member)) {
        iterator.remove();
      }
    }
    members.forEach(this::addMember);
    return this;
  }

  /**
   * Sets the set of members.
   *
   * @param members A collection of members to set.
   * @return The Raft context.
   */
  RaftContext updateMembers(Collection<RaftMember> members) {
    members.forEach(this::addMember);
    return this;
  }

  /**
   * Adds a member to the cluster.
   *
   * @param member The member to add.
   * @return The Raft context.
   */
  RaftContext addMember(RaftMember member) {
    RaftMember m = members.get(member.uri());
    if (m != null) {
      m.update(member);
    } else {
      members.put(member.uri(), member);
    }
    return this;
  }

  /**
   * Removes a member from the cluster.
   *
   * @param member The member to remove.
   * @return The Raft context.
   */
  RaftContext removeMember(RaftMember member) {
    members.remove(member.uri());
    return this;
  }

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   * @return The Raft context.
   */
  RaftContext setLeader(String leader) {
    if (this.leader == null) {
      if (leader != null) {
        this.leader = leader;
        this.lastVotedFor = null;
        LOGGER.debug("{} - Found leader {}", localMember, leader);
        if (openFuture != null) {
          openFuture.complete(null);
          openFuture = null;
        }
        triggerChangeEvent();
      }
    } else if (leader != null) {
      if (!this.leader.equals(leader)) {
        this.leader = leader;
        this.lastVotedFor = null;
        LOGGER.debug("{} - Found leader {}", localMember, leader);
        triggerChangeEvent();
      }
    } else {
      this.leader = null;
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
   * @return The Raft context.
   */
  RaftContext setTerm(long term) {
    if (term > this.term) {
      this.term = term;
      this.leader = null;
      this.lastVotedFor = null;
      LOGGER.debug("{} - Incremented term {}", localMember, term);
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
   * @return The Raft context.
   */
  RaftContext setVersion(long version) {
    this.version = Math.max(this.version, version);
    localMember.version(this.version);
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
   * Returns whether the context is recovering.
   *
   * @return Indicates whether the context is currently recovering.
   */
  public boolean isRecovering() {
    return recovering;
  }

  /**
   * Sets the state last voted for candidate.
   *
   * @param candidate The candidate that was voted for.
   * @return The Raft context.
   */
  RaftContext setLastVotedFor(String candidate) {
    // If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
    if (lastVotedFor != null && candidate != null) {
      throw new IllegalStateException("Already voted for another candidate");
    }
    if (leader != null && candidate != null) {
      throw new IllegalStateException("Cannot cast vote - leader already exists");
    }
    this.lastVotedFor = candidate;
    if (candidate != null) {
      LOGGER.debug("{} - Voted for {}", localMember, candidate);
    } else {
      LOGGER.debug("{} - Reset last voted for", localMember);
    }
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
   * @return The Raft context.
   */
  RaftContext setCommitIndex(Long commitIndex) {
    if (firstCommitIndex == null) {
      firstCommitIndex = commitIndex;
    }
    this.commitIndex = this.commitIndex != null ? Assert.arg(Assert.isNotNull(commitIndex, "commitIndex"), commitIndex >= this.commitIndex, "cannot decrease commit index") : commitIndex;
    localMember.index(this.commitIndex);
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
   * @return The Raft context.
   */
  RaftContext setLastApplied(Long lastApplied) {
    this.lastApplied = this.lastApplied != null ? Assert.arg(Assert.isNotNull(lastApplied, "lastApplied"), lastApplied >= this.lastApplied, "cannot decrease last applied index") : lastApplied;
    if (recovering && this.lastApplied != null && firstCommitIndex != null && this.lastApplied >= firstCommitIndex) {
      recovering = false;
    }
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
   * @return The Raft context.
   */
  RaftContext setElectionTimeout(long electionTimeout) {
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
   * @return The Raft context.
   */
  RaftContext setHeartbeatInterval(long heartbeatInterval) {
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
  public RaftContext consumer(TriFunction<Long, Long, ByteBuffer, ByteBuffer> consumer) {
    this.consumer = consumer;
    return this;
  }

  /**
   * Returns the log consumer.
   *
   * @return The log consumer.
   */
  public TriFunction<Long, Long, ByteBuffer, ByteBuffer> consumer() {
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
  public CompletableFuture<JoinResponse> join(JoinRequest request) {
    return wrapCall(request, state::join);
  }

  @Override
  public RaftProtocol joinHandler(MessageHandler<JoinRequest, JoinResponse> handler) {
    this.joinHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<PromoteResponse> promote(PromoteRequest request) {
    return wrapCall(request, state::promote);
  }

  @Override
  public RaftProtocol promoteHandler(MessageHandler<PromoteRequest, PromoteResponse> handler) {
    this.promoteHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    return wrapCall(request, state::leave);
  }

  @Override
  public RaftProtocol leaveHandler(MessageHandler<LeaveRequest, LeaveResponse> handler) {
    this.leaveHandler = handler;
    return this;
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
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return wrapCall(request, state::poll);
  }

  @Override
  public RaftProtocol pollHandler(MessageHandler<PollRequest, PollResponse> handler) {
    this.pollHandler = handler;
    return this;
  }

  @Override
  public RaftContext voteHandler(MessageHandler<VoteRequest, VoteResponse> handler) {
    this.voteHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    return wrapCall(request, state::vote);
  }

  @Override
  public RaftContext appendHandler(MessageHandler<AppendRequest, AppendResponse> handler) {
    this.appendHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    return wrapCall(request, state::append);
  }

  @Override
  public RaftContext queryHandler(MessageHandler<QueryRequest, QueryResponse> handler) {
    this.queryHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    return wrapCall(request, state::query);
  }

  @Override
  public RaftContext commitHandler(MessageHandler<CommitRequest, CommitResponse> handler) {
    this.commitHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<CommitResponse> commit(CommitRequest request) {
    return wrapCall(request, state::commit);
  }

  /**
   * Wraps a call to the state context in the context executor.
   */
  private <T extends Request, U extends Response> CompletableFuture<U> wrapCall(T request, MessageHandler<T, U> handler) {
    CompletableFuture<U> future = new CompletableFuture<>();
    executor.execute(() -> {
      handler.apply(request).whenComplete((response, error) -> {
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
   * Checks that the current thread is the state context thread.
   */
  void checkThread() {
    if (Thread.currentThread() != thread) {
      throw new IllegalStateException("State not running on correct thread");
    }
  }

  /**
   * Transition registerHandler.
   */
  CompletableFuture<RaftState.Type> transition(RaftState.Type state) {
    checkThread();

    if (this.state != null && state == this.state.type()) {
      return CompletableFuture.completedFuture(this.state.type());
    }

    LOGGER.info("{} - Transitioning to {}", localMember, state);

    // Force state transitions to occur synchronously in order to prevent race conditions.
    if (this.state != null) {
      try {
        this.state.close().get();
        this.state = state.type().getConstructor(RaftContext.class).newInstance(this);
        registerHandlers(this.state);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new CopycatException(e);
      }
    } else {
      // Force state transitions to occur synchronously in order to prevent race conditions.
      try {
        this.state = state.type().getConstructor(RaftContext.class).newInstance(this);
        registerHandlers(this.state);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new CopycatException(e);
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Registers handlers on the given state.
   */
  private void registerHandlers(RaftState state) {
    state.joinHandler(joinHandler);
    state.leaveHandler(leaveHandler);
    state.syncHandler(syncHandler);
    state.appendHandler(appendHandler);
    state.pollHandler(pollHandler);
    state.voteHandler(voteHandler);
    state.queryHandler(queryHandler);
    state.commitHandler(commitHandler);
    state.transitionHandler(this::transition);
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
        transition(localMember.type() == Member.Type.PASSIVE ? RaftState.Type.PASSIVE : RaftState.Type.FOLLOWER);
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
  public synchronized CompletableFuture<Void> close() {
    if (openFuture != null) {
      openFuture.cancel(false);
      openFuture = null;
    } else if (!open) {
      return Futures.exceptionalFuture(new IllegalStateException("Context not open"));
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    executor.execute(() -> {
      transition(RaftState.Type.START).whenComplete((result, error) -> {
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
