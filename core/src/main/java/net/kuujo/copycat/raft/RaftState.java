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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.raft.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class RaftState implements RaftProtocol {
  protected static final byte ENTRY_TYPE_USER = 0;
  protected static final byte ENTRY_TYPE_CONFIG = 1;
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final RaftContext context;
  protected MessageHandler<JoinRequest, JoinResponse> joinHandler;
  protected MessageHandler<PromoteRequest, PromoteResponse> promoteHandler;
  protected MessageHandler<LeaveRequest, LeaveResponse> leaveHandler;
  protected MessageHandler<SyncRequest, SyncResponse> syncHandler;
  protected MessageHandler<PollRequest, PollResponse> pollHandler;
  protected MessageHandler<VoteRequest, VoteResponse> voteHandler;
  protected MessageHandler<AppendRequest, AppendResponse> appendHandler;
  protected MessageHandler<CommitRequest, CommitResponse> commitHandler;
  protected MessageHandler<QueryRequest, QueryResponse> queryHandler;
  protected MessageHandler<Type, Type> transitionHandler;
  private volatile boolean open;

  protected RaftState(RaftContext context) {
    this.context = context;
  }

  /**
   * Raft state types.
   */
  public static enum Type {

    /**
     * Start state.
     */
    START(StartState.class),

    /**
     * Passive state.
     */
    PASSIVE(PassiveState.class),

    /**
     * Follower state.
     */
    FOLLOWER(FollowerState.class),

    /**
     * Candidate state.
     */
    CANDIDATE(CandidateState.class),

    /**
     * Leader state.
     */
    LEADER(LeaderState.class);

    private final Class<? extends RaftState> type;

    private Type(Class<? extends RaftState> type) {
      this.type = type;
    }

    /**
     * Returns the state type class.
     *
     * @return The state type clas.
     */
    public Class<? extends RaftState> type() {
      return type;
    }
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
  public abstract Type type();

  /**
   * Logs a request.
   */
  protected final <R extends Request> R logRequest(R request) {
    LOGGER.debug("{} - Received {}", context.getLocalMember(), request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends Response> R logResponse(R response) {
    LOGGER.debug("{} - Sent {}", context.getLocalMember(), response);
    return response;
  }

  @Override
  public CompletableFuture<JoinResponse> join(JoinRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  @Override
  public RaftProtocol joinHandler(MessageHandler<JoinRequest, JoinResponse> handler) {
    this.joinHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<PromoteResponse> promote(PromoteRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  @Override
  public RaftProtocol promoteHandler(MessageHandler<PromoteRequest, PromoteResponse> handler) {
    this.promoteHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  @Override
  public RaftProtocol leaveHandler(MessageHandler<LeaveRequest, LeaveResponse> handler) {
    this.leaveHandler = handler;
    return this;
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
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  @Override
  public RaftProtocol pollHandler(MessageHandler<PollRequest, PollResponse> handler) {
    this.pollHandler = handler;
    return this;
  }

  @Override
  public RaftState voteHandler(MessageHandler<VoteRequest, VoteResponse> handler) {
    this.voteHandler = handler;
    return this;
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    return exceptionalFuture(new IllegalStateException("Invalid Copycat state"));
  }

  @Override
  public RaftState appendHandler(MessageHandler<AppendRequest, AppendResponse> handler) {
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
  public RaftState commitHandler(MessageHandler<CommitRequest, CommitResponse> handler) {
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
  public RaftState transitionHandler(MessageHandler<Type, Type> handler) {
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

  @Override
  public String toString() {
    return String.format("%s[context=%s]", getClass().getSimpleName(), context);
  }

}
