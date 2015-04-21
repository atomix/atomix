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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.util.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class RaftState implements ProtocolHandler<Request, Response>, Managed<RaftState> {
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final RaftContext context;
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
     * Remote state.
     */
    REMOTE(RemoteState.class),

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
     * @return The state type class.
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
    LOGGER.debug("{} - Received {}", context.getLocalMember().id(), request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends Response> R logResponse(R response) {
    LOGGER.debug("{} - Sent {}", context.getLocalMember().id(), response);
    return response;
  }

  @Override
  public CompletableFuture<RaftState> open() {
    context.checkThread();
    open = true;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<? extends Response> apply(Request request) {
    context.checkThread();
    switch (request.type()) {
      case APPEND:
        return append(request.asAppendRequest());
      case SYNC:
        return sync(request.asSyncRequest());
      case POLL:
        return poll(request.asPollRequest());
      case VOTE:
        return vote(request.asVoteRequest());
      case WRITE:
        return write(request.asWriteRequest());
      case READ:
        return read(request.asReadRequest());
      case DELETE:
        return delete(request.asDeleteRequest());
    }
    throw new IllegalArgumentException("invalid request type");
  }

  /**
   * Handles an append request.
   */
  protected abstract CompletableFuture<AppendResponse> append(AppendRequest request);

  /**
   * Handles a sync request.
   */
  protected abstract CompletableFuture<SyncResponse> sync(SyncRequest request);

  /**
   * Handles a poll request.
   */
  protected abstract CompletableFuture<PollResponse> poll(PollRequest request);

  /**
   * Handles a vote request.
   */
  protected abstract CompletableFuture<VoteResponse> vote(VoteRequest request);

  /**
   * Handles a write request.
   */
  protected abstract CompletableFuture<WriteResponse> write(WriteRequest request);

  /**
   * Handles a read request.
   */
  protected abstract CompletableFuture<ReadResponse> read(ReadRequest request);

  /**
   * Handles a delete request.
   */
  protected abstract CompletableFuture<DeleteResponse> delete(DeleteRequest request);

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
