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
package net.kuujo.copycat.raft.state;

import net.kuujo.copycat.io.storage.Entry;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.protocol.error.InternalException;
import net.kuujo.copycat.raft.protocol.error.UnknownSessionException;
import net.kuujo.copycat.raft.session.Session;
import net.kuujo.copycat.raft.storage.*;
import net.kuujo.copycat.util.concurrent.ComposableFuture;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Raft server state machine.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ServerStateMachine implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStateMachine.class);
  private final StateMachine stateMachine;
  private final ServerStateMachineExecutor executor;
  private final ServerCommitPool commits;
  private Duration sessionTimeout;
  private long lastApplied;

  ServerStateMachine(StateMachine stateMachine, ServerCommitCleaner cleaner, Context context) {
    this.stateMachine = stateMachine;
    this.executor = new ServerStateMachineExecutor(context);
    this.commits = new ServerCommitPool(cleaner, executor.context().sessions());
    init();
  }

  /**
   * Initializes the state machine.
   */
  private void init() {
    stateMachine.init(executor.context());
    stateMachine.configure(executor);
  }

  /**
   * Returns the server state machine executor.
   *
   * @return The server state machine executor.
   */
  ServerStateMachineExecutor executor() {
    return executor;
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  Duration getSessionTimeout() {
    return sessionTimeout;
  }

  /**
   * Sets the session timeout.
   *
   * @param sessionTimeout The session timeout.
   */
  void setSessionTimeout(Duration sessionTimeout) {
    if (sessionTimeout == null)
      throw new NullPointerException("sessionTimeout cannot be null");
    this.sessionTimeout = sessionTimeout;
  }

  /**
   * Returns the last applied index.
   *
   * @return The last applied inex.
   */
  long getLastApplied() {
    return lastApplied;
  }

  /**
   * Sets the last applied inex.
   *
   * @param lastApplied The last applied index.
   */
  private void setLastApplied(long lastApplied) {
    if (lastApplied < this.lastApplied) {
      throw new IllegalArgumentException("lastApplied index must be greater than previous lastApplied index");
    } else if (lastApplied > this.lastApplied) {
      this.lastApplied = lastApplied;
      for (ServerSession session : executor.context().sessions().sessions.values()) {
        session.setVersion(lastApplied);
      }
    }
  }

  /**
   * Returns the current thread context.
   *
   * @return The current thread context.
   */
  private Context getContext() {
    Context context = Context.currentContext();
    if (context == null)
      throw new IllegalStateException("must be called from a Copycat thread");
    return context;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<?> apply(Entry entry) {
    try {
      if (entry instanceof CommandEntry) {
        return apply((CommandEntry) entry);
      } else if (entry instanceof QueryEntry) {
        return apply((QueryEntry) entry);
      } else if (entry instanceof RegisterEntry) {
        return apply((RegisterEntry) entry);
      } else if (entry instanceof KeepAliveEntry) {
        return apply((KeepAliveEntry) entry);
      } else if (entry instanceof NoOpEntry) {
        return apply((NoOpEntry) entry);
      }
      return Futures.exceptionalFuture(new InternalException("unknown state machine operation"));
    } finally {
      setLastApplied(entry.getIndex());
    }
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  private CompletableFuture<Long> apply(RegisterEntry entry) {
    ServerSession session = executor.context().sessions().registerSession(entry.getIndex(), entry.getConnection()).setTimestamp(entry.getTimestamp());

    Context context = getContext();

    // Set last applied only after the operation has been submitted to the state machine executor.
    CompletableFuture<Long> future = new ComposableFuture<>();
    executor.executor().execute(() -> {
      stateMachine.register(session);
      context.execute(() -> future.complete(entry.getIndex()));
    });

    // Expire any remaining expired sessions.
    expireSessions(entry.getTimestamp());

    // Allow the executor to execute any scheduled events.
    executor.tick(entry.getTimestamp());

    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   */
  private CompletableFuture<Void> apply(KeepAliveEntry entry) {
    ServerSession session = executor.context().sessions().getSession(entry.getSession());

    CompletableFuture<Void> future;

    // If the server session is null, the session either never existed or already expired.
    if (session == null) {
      LOGGER.warn("Unknown session: " + entry.getSession());
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session: " + entry.getSession()));
    }
    // If the session exists, don't allow it to expire even if its expiration has passed since we still
    // managed to receive a keep alive request from the client before it was removed.
    else {
      Context context = getContext();

      // The keep alive request contains the
      session.setTimestamp(entry.getTimestamp())
        .clearResponses(entry.getCommandSequence())
        .clearEvents(entry.getEventSequence());

      future = new CompletableFuture<>();
      context.execute(() -> future.complete(null));
    }

    // Expire any remaining expired sessions.
    expireSessions(entry.getTimestamp());

    // Allow the executor to execute any scheduled events.
    executor.tick(entry.getTimestamp());

    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Object> apply(CommandEntry entry) {
    final CompletableFuture<Object> future;

    // First check to ensure that the session exists.
    ServerSession session = executor.context().sessions().getSession(entry.getSession());

    // If the session is null then that indicates that the session already timed out or it never existed.
    // Return with an UnknownSessionException.
    if (session == null) {
      LOGGER.warn("Unknown session: " + entry.getSession());
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session " + entry.getSession()));
    }
    // If the command's sequence number is greater than the next session sequence number then that indicates that
    // we've received the command out of sequence. Queue the command to be applied in the correct order.
    else if (entry.getSequence() > session.nextSequence()) {
      future = new CompletableFuture<>();
      Context context = getContext();
      session.registerCommand(entry.getSequence(), () -> executeCommand(entry, session, future, context));
    }
    // If the command's sequence number is less than the next session sequence number then that indicates that
    // we've received a command that was previously applied to the state machine. Ensure linearizability by
    // returning the cached response instead of applying it to the user defined state machine.
    else if (entry.getSequence() < session.nextSequence()) {
      future = new CompletableFuture<>();

      // Ensure the response check is executed in the state machine thread in order to ensure the
      // command was applied, otherwise there will be a race condition and concurrent modification issues.
      Context context = getContext();
      executor.executor().execute(() -> {
        Object response = session.getResponse(entry.getSequence());
        if (response == null) {
          context.executor().execute(() -> future.complete(null));
        } else if (response instanceof Throwable) {
          context.executor().execute(() -> future.completeExceptionally((Throwable) response));
        } else {
          context.executor().execute(() -> future.complete(response));
        }
      });
    }
    // If we've made it this far, the command must have been applied in the proper order as sequenced by the
    // session. This should be the case for most commands applied to the state machine.
    else {
      future = new CompletableFuture<>();
      executeCommand(entry, session, future, getContext());
    }

    return future;
  }

  /**
   * Executes a state machine command.
   */
  private CompletableFuture<Object> executeCommand(CommandEntry entry, ServerSession session, CompletableFuture<Object> future, Context context) {
    context.checkThread();

    long sequence = entry.getSequence();

    // Execute the command in the state machine thread. Once complete, the CompletableFuture callback will be completed
    // in the state machine thread. Register the result in that thread and then complete the future in the caller's thread.
    executor.execute(commits.acquire(entry)).whenComplete((result, error) -> {
      if (error == null) {
        session.registerResponse(sequence, result);
        context.execute(() -> future.complete(result));
      } else {
        session.registerResponse(sequence, error);
        context.execute(() -> future.completeExceptionally((Throwable) error));
      }
    });

    // Update the session timestamp and command sequence number. This is done in the caller's thread since all
    // timestamp/version/sequence checks are done in this thread prior to executing operations on the state machine thread.
    session.setTimestamp(entry.getTimestamp()).setSequence(sequence);

    // Allow the executor to execute any scheduled events.
    executor.tick(entry.getTimestamp());

    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  @SuppressWarnings("unchecked")
  CompletableFuture<Object> apply(QueryEntry entry) {
    ServerSession session = executor.context().sessions().getSession(entry.getSession());
    if (session == null) {
      LOGGER.warn("Unknown session: " + entry.getSession());
      return Futures.exceptionalFuture(new UnknownSessionException("unknown session " + entry.getSession()));
    } else if (entry.getTimestamp() - sessionTimeout.toMillis() > session.getTimestamp()) {
      LOGGER.warn("Expired session: " + entry.getSession());
      return expireSession(entry.getSession(), getContext());
    } else if (session.getVersion() < entry.getVersion()) {
      ComposableFuture<Object> future = new ComposableFuture<>();

      // Get the caller's context.
      Context context = getContext();

      session.registerQuery(entry.getVersion(), () -> {
        context.checkThread();
        executeQuery(entry, future, context);
      });
      return future;
    } else {
      return executeQuery(entry, new CompletableFuture<>(), getContext());
    }
  }

  /**
   * Executes a state machine query.
   */
  private CompletableFuture<Object> executeQuery(QueryEntry entry, CompletableFuture<Object> future, Context context) {
    executor.execute(commits.acquire(entry)).whenComplete((result, error) -> {
      if (error == null) {
        context.execute(() -> future.complete(result));
      } else {
        context.execute(() -> future.completeExceptionally((Throwable) error));
      }
    });
    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<Long> apply(NoOpEntry entry) {
    // We need to ensure that the command is applied to the state machine before queries are run.
    // Set last applied only after the operation has been submitted to the state machine executor.
    return Futures.completedFutureAsync(entry.getIndex(), getContext().executor());
  }

  /**
   * Expires any sessions that have timed out.
   */
  private void expireSessions(long timestamp) {
    for (Session s : executor.context().sessions()) {
      if (timestamp - sessionTimeout.toMillis() > ((ServerSession) s).getTimestamp()) {
        ((ServerSession) s).expire();
        stateMachine.expire(s);
      }
    }
  }

  /**
   * Expires a session.
   */
  private <T> CompletableFuture<T> expireSession(long sessionId, Context context) {
    CompletableFuture<T> future = new CompletableFuture<>();
    ServerSession session = executor.context().sessions().unregisterSession(sessionId);
    if (session != null) {
      executor.executor().execute(() -> {
        session.expire();
        stateMachine.expire(session);
        context.executor().execute(() -> future.completeExceptionally(new UnknownSessionException("unknown session: " + sessionId)));
      });
    } else {
      context.executor().execute(() -> future.completeExceptionally(new UnknownSessionException("unknown session: " + sessionId)));
    }
    return future;
  }

  @Override
  public void close() {
    executor.close();
  }

}
