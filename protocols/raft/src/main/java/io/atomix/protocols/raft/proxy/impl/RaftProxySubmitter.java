/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.proxy.impl;

import io.atomix.logging.Logger;
import io.atomix.logging.LoggerFactory;
import io.atomix.protocols.raft.RaftCommand;
import io.atomix.protocols.raft.RaftQuery;
import io.atomix.protocols.raft.error.CommandException;
import io.atomix.protocols.raft.error.QueryException;
import io.atomix.protocols.raft.error.RaftError;
import io.atomix.protocols.raft.error.UnknownSessionException;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.OperationRequest;
import io.atomix.protocols.raft.protocol.OperationResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.serializer.Serializer;
import io.atomix.utils.concurrent.ThreadContext;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Session operation submitter.
 */
final class RaftProxySubmitter {
  private static final Logger LOG = LoggerFactory.getLogger(RaftProxySubmitter.class);
  private static final int[] FIBONACCI = new int[]{1, 1, 2, 3, 5};
  private static final Predicate<Throwable> EXCEPTION_PREDICATE = e ->
      e instanceof ConnectException
          || e instanceof TimeoutException
          || e instanceof ClosedChannelException;

  private final RaftProxyConnection leaderConnection;
  private final RaftProxyConnection sessionConnection;
  private final RaftProxyState state;
  private final RaftProxySequencer sequencer;
  private final RaftProxyManager manager;
  private final Serializer serializer;
  private final ThreadContext context;
  private final Map<Long, OperationAttempt> attempts = new LinkedHashMap<>();
  private final AtomicLong keepAliveIndex = new AtomicLong();

  public RaftProxySubmitter(
      RaftProxyConnection leaderConnection,
      RaftProxyConnection sessionConnection,
      RaftProxyState state,
      RaftProxySequencer sequencer,
      RaftProxyManager manager,
      Serializer serializer,
      ThreadContext context) {
    this.leaderConnection = checkNotNull(leaderConnection, "leaderConnection");
    this.sessionConnection = checkNotNull(sessionConnection, "sessionConnection");
    this.state = checkNotNull(state, "state");
    this.sequencer = checkNotNull(sequencer, "sequencer");
    this.manager = checkNotNull(manager, "manager");
    this.serializer = checkNotNull(serializer, "serializer cannot be null");
    this.context = checkNotNull(context, "context cannot be null");
  }

  /**
   * Submits a command to the cluster.
   *
   * @param command The command to submit.
   * @param <T>     The command result type.
   * @return A completable future to be completed once the command has been submitted.
   */
  public <T> CompletableFuture<T> submit(RaftCommand<T> command) {
    CompletableFuture<T> future = new CompletableFuture<>();
    context.execute(() -> submitCommand(command, future));
    return future;
  }

  /**
   * Submits a command to the cluster.
   */
  private <T> void submitCommand(RaftCommand<T> command, CompletableFuture<T> future) {
    byte[] bytes = serializer.encode(command);
    CommandRequest request = CommandRequest.builder()
        .withSession(state.getSessionId())
        .withSequence(state.nextCommandRequest())
        .withBytes(bytes)
        .build();
    submitCommand(request, future);
  }

  /**
   * Submits a command request to the cluster.
   */
  private <T> void submitCommand(CommandRequest request, CompletableFuture<T> future) {
    submit(new CommandAttempt<>(sequencer.nextRequest(), request, future));
  }

  /**
   * Submits a query to the cluster.
   *
   * @param query The query to submit.
   * @param <T>   The query result type.
   * @return A completable future to be completed once the query has been submitted.
   */
  public <T> CompletableFuture<T> submit(RaftQuery<T> query) {
    CompletableFuture<T> future = new CompletableFuture<>();
    context.execute(() -> submitQuery(query, future));
    return future;
  }

  /**
   * Submits a query to the cluster.
   */
  private <T> void submitQuery(RaftQuery<T> query, CompletableFuture<T> future) {
    byte[] bytes = serializer.encode(query);
    QueryRequest request = QueryRequest.builder()
        .withSession(state.getSessionId())
        .withSequence(state.getCommandRequest())
        .withIndex(state.getResponseIndex())
        .withBytes(bytes)
        .withConsistency(query.consistency())
        .build();
    submitQuery(request, future);
  }

  /**
   * Submits a query request to the cluster.
   */
  private <T> void submitQuery(QueryRequest request, CompletableFuture<T> future) {
    submit(new QueryAttempt<>(sequencer.nextRequest(), request, future));
  }

  /**
   * Submits an operation attempt.
   *
   * @param attempt The attempt to submit.
   */
  private <T extends OperationRequest, U extends OperationResponse, V> void submit(OperationAttempt<T, U, V> attempt) {
    if (state.getState() == RaftProxy.State.CLOSED) {
      attempt.fail(new UnknownSessionException("session closed"));
    } else {
      LOG.trace("{} - Sending {}", state.getSessionId(), attempt.request);
      attempts.put(attempt.sequence, attempt);
      attempt.send();
      attempt.future.whenComplete((r, e) -> attempts.remove(attempt.sequence));
    }
  }

  /**
   * Resubmits commands starting after the given sequence number.
   * <p>
   * The sequence number from which to resend commands is the <em>request</em> sequence number,
   * not the client-side sequence number. We resend only commands since queries cannot be reliably
   * resent without losing linearizable semantics. Commands are resent by iterating through all pending
   * operation attempts and retrying commands where the sequence number is greater than the given
   * {@code commandSequence} number and the attempt number is less than or equal to the version.
   */
  private void resubmit(long commandSequence, OperationAttempt<?, ?, ?> attempt) {
    // If the client's response sequence number is greater than the given command sequence number,
    // the cluster likely has a new leader, and we need to reset the sequencing in the leader by
    // sending a keep-alive request.
    // Ensure that the client doesn't resubmit many concurrent KeepAliveRequests by tracking the last
    // keep-alive response sequence number and only resubmitting if the sequence number has changed.
    long responseSequence = state.getCommandResponse();
    if (commandSequence < responseSequence && keepAliveIndex.get() != responseSequence) {
      keepAliveIndex.set(responseSequence);
      manager.resetIndexes(state.getSessionId()).whenCompleteAsync((result, error) -> {
        if (error == null) {
          resubmit(responseSequence, attempt);
        } else {
          attempt.retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt.attempt - 1, FIBONACCI.length - 1)]));
        }
      }, context);
    } else {
      for (Map.Entry<Long, OperationAttempt> entry : attempts.entrySet()) {
        OperationAttempt operation = entry.getValue();
        if (operation instanceof CommandAttempt && operation.request.sequence() > commandSequence && operation.attempt <= attempt.attempt) {
          operation.retry();
        }
      }
    }
  }

  /**
   * Closes the submitter.
   *
   * @return A completable future to be completed with a list of pending operations.
   */
  public CompletableFuture<Void> close() {
    for (OperationAttempt attempt : new ArrayList<>(attempts.values())) {
      attempt.fail(new UnknownSessionException("session closed"));
    }
    attempts.clear();
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Operation attempt.
   */
  private abstract class OperationAttempt<T extends OperationRequest, U extends OperationResponse, V> implements BiConsumer<U, Throwable> {
    protected final long sequence;
    protected final int attempt;
    protected final T request;
    protected final CompletableFuture<V> future;

    protected OperationAttempt(long sequence, int attempt, T request, CompletableFuture<V> future) {
      this.sequence = sequence;
      this.attempt = attempt;
      this.request = request;
      this.future = future;
    }

    /**
     * Sends the attempt.
     */
    protected abstract void send();

    /**
     * Returns the next instance of the attempt.
     *
     * @return The next instance of the attempt.
     */
    protected abstract OperationAttempt<T, U, V> next();

    /**
     * Returns a new instance of the default exception for the operation.
     *
     * @return A default exception for the operation.
     */
    protected abstract Throwable defaultException();

    /**
     * Completes the operation successfully.
     *
     * @param response The operation response.
     */
    protected abstract void complete(U response);

    /**
     * Completes the operation with an exception.
     *
     * @param error The completion exception.
     */
    protected void complete(Throwable error) {
      sequence(null, () -> future.completeExceptionally(error));
    }

    /**
     * Runs the given callback in proper sequence.
     *
     * @param response The operation response.
     * @param callback The callback to run in sequence.
     */
    protected final void sequence(OperationResponse response, Runnable callback) {
      sequencer.sequenceResponse(sequence, response, callback);
    }

    /**
     * Fails the attempt.
     */
    public void fail() {
      fail(defaultException());
    }

    /**
     * Fails the attempt with the given exception.
     *
     * @param t The exception with which to fail the attempt.
     */
    public void fail(Throwable t) {
      complete(t);
    }

    /**
     * Immediately retries the attempt.
     */
    public void retry() {
      context.execute(() -> submit(next()));
    }

    /**
     * Retries the attempt after the given duration.
     *
     * @param after The duration after which to retry the attempt.
     */
    public void retry(Duration after) {
      context.schedule(after, () -> submit(next()));
    }
  }

  /**
   * Command operation attempt.
   */
  private final class CommandAttempt<T> extends OperationAttempt<CommandRequest, CommandResponse, T> {
    public CommandAttempt(long sequence, CommandRequest request, CompletableFuture<T> future) {
      super(sequence, 1, request, future);
    }

    public CommandAttempt(long sequence, int attempt, CommandRequest request, CompletableFuture<T> future) {
      super(sequence, attempt, request, future);
    }

    @Override
    protected void send() {
      leaderConnection.command(request).whenComplete(this);
    }

    @Override
    protected OperationAttempt<CommandRequest, CommandResponse, T> next() {
      return new CommandAttempt<>(sequence, this.attempt + 1, request, future);
    }

    @Override
    protected Throwable defaultException() {
      return new CommandException("failed to complete command");
    }

    @Override
    public void accept(CommandResponse response, Throwable error) {
      if (error == null) {
        LOG.trace("{} - Received {}", state.getSessionId(), response);
        if (response.status() == RaftResponse.Status.OK) {
          complete(response);
        }
        // COMMAND_ERROR indicates that the command was received by the leader out of sequential order.
        // We need to resend commands starting at the provided lastSequence number.
        else if (response.error() == RaftError.Type.COMMAND_ERROR) {
          resubmit(response.lastSequence(), this);
        }
        // The following exceptions need to be handled at a higher level by the client or the user.
        else if (response.error() == RaftError.Type.APPLICATION_ERROR
            || response.error() == RaftError.Type.UNKNOWN_CLIENT_ERROR
            || response.error() == RaftError.Type.UNKNOWN_SESSION_ERROR
            || response.error() == RaftError.Type.UNKNOWN_STATE_MACHINE_ERROR
            || response.error() == RaftError.Type.INTERNAL_ERROR) {
          complete(response.error().createException());
        }
        // For all other errors, use fibonacci backoff to resubmit the command.
        else {
          retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
        }
      } else if (EXCEPTION_PREDICATE.test(error) || (error instanceof CompletionException && EXCEPTION_PREDICATE.test(error.getCause()))) {
        retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
      } else {
        fail(error);
      }
    }

    @Override
    public void fail(Throwable cause) {
      super.fail(cause);
      if (cause instanceof UnknownSessionException) {
        state.setState(RaftProxy.State.CLOSED);
        CommandRequest request = CommandRequest.builder()
            .withSession(this.request.session())
            .withSequence(this.request.sequence())
            .withBytes(new byte[0])
            .build();
        context.execute(() -> submit(new CommandAttempt<>(sequence, this.attempt + 1, request, future)));
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void complete(CommandResponse response) {
      sequence(response, () -> {
        state.setCommandResponse(request.sequence());
        state.setResponseIndex(response.index());
        future.complete((T) response.result());
      });
    }
  }

  /**
   * Query operation attempt.
   */
  private final class QueryAttempt<T> extends OperationAttempt<QueryRequest, QueryResponse, T> {
    public QueryAttempt(long sequence, QueryRequest request, CompletableFuture<T> future) {
      super(sequence, 1, request, future);
    }

    public QueryAttempt(long sequence, int attempt, QueryRequest request, CompletableFuture<T> future) {
      super(sequence, attempt, request, future);
    }

    @Override
    protected void send() {
      sessionConnection.query(request).whenComplete(this);
    }

    @Override
    protected OperationAttempt<QueryRequest, QueryResponse, T> next() {
      return new QueryAttempt<>(sequence, this.attempt + 1, request, future);
    }

    @Override
    protected Throwable defaultException() {
      return new QueryException("failed to complete query");
    }

    @Override
    public void accept(QueryResponse response, Throwable error) {
      if (error == null) {
        LOG.trace("{} - Received {}", state.getSessionId(), response);
        if (response.status() == RaftResponse.Status.OK) {
          complete(response);
        } else {
          complete(response.error().createException());
        }
      } else {
        fail(error);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void complete(QueryResponse response) {
      sequence(response, () -> {
        state.setResponseIndex(response.index());
        future.complete((T) response.result());
      });
    }
  }

}
