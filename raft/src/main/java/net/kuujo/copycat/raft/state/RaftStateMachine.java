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

import net.kuujo.copycat.cluster.MemberInfo;
import net.kuujo.copycat.cluster.Session;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.log.compact.Compaction;
import net.kuujo.copycat.raft.log.entry.*;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.ThreadChecker;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * Resource state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class RaftStateMachine {
  private final StateMachine stateMachine;
  private final RaftContext state;
  private final ExecutionContext context;
  private final ThreadChecker threadChecker;
  private final Map<Long, RaftSession> sessions = new HashMap<>();
  private long sessionTimeout = 5000;

  public RaftStateMachine(StateMachine stateMachine, RaftContext state, ExecutionContext context) {
    this.stateMachine = stateMachine;
    this.state = state;
    this.context = context;
    this.threadChecker = new ThreadChecker(context);
  }

  /**
   * Filters an entry.
   *
   * @param entry The entry to filter.
   * @return A completable future to be completed with a boolean value indicating whether to keep the entry.
   */
  public CompletableFuture<Boolean> filter(RaftEntry entry, Compaction compaction) {
    if (entry instanceof OperationEntry) {
      return filter((OperationEntry) entry, compaction);
    } else if (entry instanceof RegisterEntry) {
      return filter((RegisterEntry) entry, compaction);
    } else if (entry instanceof KeepAliveEntry) {
      return filter((KeepAliveEntry) entry, compaction);
    }
    return CompletableFuture.completedFuture(false);
  }

  /**
   * Filters an entry.
   *
   * @param entry The entry to filter.
   * @return A completable future to be completed with a boolean value indicating whether to keep the entry.
   */
  public CompletableFuture<Boolean> filter(RegisterEntry entry, Compaction compaction) {
    return CompletableFuture.supplyAsync(() -> {
      return sessions.containsKey(entry.getIndex());
    }, context);
  }

  /**
   * Filters an entry.
   *
   * @param entry The entry to filter.
   * @return A completable future to be completed with a boolean value indicating whether to keep the entry.
   */
  public CompletableFuture<Boolean> filter(KeepAliveEntry entry, Compaction compaction) {
    return CompletableFuture.supplyAsync(() -> {
      return sessions.containsKey(entry.getIndex()) && sessions.get(entry.getIndex()).index == entry.getIndex();
    }, context);
  }

  /**
   * Filters a no-op entry.
   *
   * @param entry The entry to filter.
   * @return A completable future to be completed with a boolean value indicating whether to keep the entry.
   */
  public CompletableFuture<Boolean> filter(NoOpEntry entry, Compaction compaction) {
    return CompletableFuture.completedFuture(false);
  }

  /**
   * Filters an entry.
   *
   * @param entry The entry to filter.
   * @return A completable future to be completed with a boolean value indicating whether to keep the entry.
   */
  public CompletableFuture<Boolean> filter(OperationEntry entry, Compaction compaction) {
    return CompletableFuture.supplyAsync(() -> {
      RaftSession session = sessions.get(entry.getSession());
      if (session == null) {
        session = new RaftSession(entry.getSession(), null, entry.getTimestamp());
        session.expire();
      }
      return stateMachine.filter(new Commit<>(entry.getIndex(), session, entry.getTimestamp(), (Command) entry.getOperation()), compaction);
    }, context);
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<?> apply(RaftEntry entry) {
    if (entry instanceof OperationEntry) {
      return apply((OperationEntry) entry);
    } else if (entry instanceof RegisterEntry) {
      return apply((RegisterEntry) entry);
    } else if (entry instanceof KeepAliveEntry) {
      return apply((KeepAliveEntry) entry);
    } else if (entry instanceof NoOpEntry) {
      return apply((NoOpEntry) entry);
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> apply(RegisterEntry entry) {
    return register(entry.getIndex(), entry.getTimestamp(), entry.getMember());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Void> apply(KeepAliveEntry entry) {
    return keepAlive(entry.getIndex(), entry.getTimestamp(), entry.getSession());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Object> apply(OperationEntry entry) {
    if (entry.getOperation() instanceof Command) {
      return command(entry.getIndex(), entry.getSession(), entry.getRequest(), entry.getResponse(), entry.getTimestamp(), (Command) entry.getOperation());
    } else {
      return query(entry.getIndex(), entry.getSession(), entry.getRequest(), entry.getResponse(), entry.getTimestamp(), (Query) entry.getOperation());
    }
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> apply(NoOpEntry entry) {
    return noop(entry.getIndex());
  }

  /**
   * Configures the state machine.
   *
   * @param members The permanent set of Raft members.
   * @param sessionTimeout The session timeout.
   * @return A completable future to be called once the state machine is configured.
   */
  private CompletableFuture<Void> configure(Set<MemberInfo> members, long sessionTimeout) {
    return CompletableFuture.runAsync(() -> {
      this.sessionTimeout = sessionTimeout;
      state.getCluster().configure(members.toArray(new MemberInfo[members.size()])).join();
    }, context);
  }

  /**
   * Registers a member session.
   *
   * @param index The registration index.
   * @param timestamp The registration timestamp.
   * @param member The member info.
   * @return A completable future to be called once the member has been registered.
   */
  private CompletableFuture<Long> register(long index, long timestamp, MemberInfo member) {
    return CompletableFuture.supplyAsync(() -> {
      RaftSession session = new RaftSession(index, member, timestamp);
      sessions.put(index, session);
      state.getCluster().register(session).join();
      stateMachine.register(session);
      return session.id();
    }, context);
  }

  /**
   * Keeps a member session alive.
   *
   * @param index The keep alive index.
   * @param timestamp The keep alive timestamp.
   * @param sessionId The session to keep alive.
   * @return A completable future to be called once the keep alive has been applied.
   */
  private CompletableFuture<Void> keepAlive(long index, long timestamp, long sessionId) {
    return CompletableFuture.runAsync(() -> {
      RaftSession session = sessions.get(sessionId);
      if (session == null) {
        throw new UnknownSessionException("unknown session: " + sessionId);
      } else if (!session.update(index, timestamp)) {
        sessions.remove(sessionId);
        stateMachine.expire(session);
        throw new UnknownSessionException("session expired: " + sessionId);
      }
    }, context);
  }

  /**
   * Applies a no-op to the state machine.
   *
   * @param index The no-op index.
   * @return A completable future to be completed once the state machine has advanced to the no-op index.
   */
  private CompletableFuture<Long> noop(long index) {
    return CompletableFuture.supplyAsync(() -> {
      if (index <= state.getLastApplied())
        throw new IllegalStateException("improperly ordered operation");
      state.setLastApplied(index);
      return index;
    }, context);
  }

  /**
   * Applies a command to the state machine.
   *
   * @param index The command index.
   * @param sessionId The command session ID.
   * @param request The command request ID.
   * @param response The command response ID.
   * @param timestamp The command timestamp.
   * @param command The command to apply.
   * @return A completable future to be completed once the command has been applied.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Object> command(long index, long sessionId, long request, long response, long timestamp, Command command) {
    return CompletableFuture.supplyAsync(() -> {

      // First check to ensure that the session exists.
      RaftSession session = sessions.get(sessionId);
      if (session == null)
        throw new UnknownSessionException("unknown session " + sessionId);

      // Given the session, check for an existing result for this command.
      if (session.responses.containsKey(request))
        return session.responses.get(request);

      // Validate that indexes are being applied in correct order.
      if (index <= state.getLastApplied())
        throw new IllegalStateException("improperly ordered operation");

      // Apply the command to the state machine.
      Object result = stateMachine.apply(new Commit(index, session, timestamp, command));
      state.setLastApplied(index);

      // Store the command result in the session.
      session.responses.put(request, result);

      // Clear any responses that have been received by the client for the session.
      session.responses.headMap(response, true).clear();

      // Return the result.
      return result;
    }, context);
  }

  /**
   * Applies a query to the state machine.
   *
   * @param index The query index.
   * @param sessionId The query session ID.
   * @param request The query request ID.
   * @param response The query response ID.
   * @param timestamp The query timestamp.
   * @param query The query to apply.
   * @return A completable future to be completed once the query has been applied.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Object> query(long index, long sessionId, long request, long response, long timestamp, Query query) {
    return CompletableFuture.supplyAsync(() -> {

      // First check to ensure that the session exists.
      RaftSession session = sessions.get(sessionId);
      if (session == null)
        throw new UnknownSessionException("unknown session " + sessionId);

      // Given the session, check for an existing result for this query.
      if (session.responses.containsKey(request))
        return session.responses.get(request);

      // In order to satisfy linearizability requirements, we only need to ensure that the query is not applied at
      // a state prior to the given index.
      if (index < state.getLastApplied())
        throw new IllegalStateException("improperly ordered query");

      // Apply the query to the state machine.
      Object result = stateMachine.apply(new Commit(index, session, timestamp, query));

      // Store the query result in the session.
      session.responses.put(request, result);

      // Update the session's timeout.
      session.update(index, timestamp);

      // Clear any responses that have been received by the client for the session.
      session.responses.headMap(response, true).clear();

      // Return the result.
      return result;
    }, context);
  }

  /**
   * Cluster session.
   */
  private class RaftSession extends Session {
    private long index;
    private long timestamp;
    private final TreeMap<Long, Object> responses = new TreeMap<>();

    private RaftSession(long id, MemberInfo member, long timestamp) {
      super(id, member);
      this.timestamp = timestamp;
    }

    /**
     * Expires the session.
     */
    public void expire() {
      super.expire();
    }

    /**
     * Returns the session timestamp.
     *
     * @return The session timestamp.
     */
    public long timestamp() {
      return timestamp;
    }

    /**
     * Updates the session.
     *
     * @param timestamp The session.
     */
    public boolean update(long index, long timestamp) {
      if (System.currentTimeMillis() - sessionTimeout > this.timestamp) {
        expire();
        return false;
      }
      this.index = index;
      this.timestamp = timestamp;
      return true;
    }

  }

}
