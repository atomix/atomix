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

import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.cluster.ManagedMember;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.MemberInfo;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.log.Compaction;
import net.kuujo.copycat.raft.log.entry.*;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.concurrent.Futures;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Resource state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class RaftStateMachine {
  private final StateMachine stateMachine;
  private final ManagedCluster cluster;
  private final ClusterState members;
  private final ExecutionContext context;
  private final Map<Long, RaftSession> sessions = new HashMap<>();
  private final Map<Long, List<Runnable>> queries = new HashMap<>();
  private long sessionTimeout = 5000;
  private long lastApplied;

  public RaftStateMachine(StateMachine stateMachine, ManagedCluster cluster, ClusterState members, ExecutionContext context) {
    this.stateMachine = stateMachine;
    this.cluster = cluster;
    this.members = members;
    this.context = context;
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  public long getSessionTimeout() {
    return sessionTimeout;
  }

  /**
   * Sets the session timeout.
   *
   * @param sessionTimeout The session timeout.
   * @return The Raft state machine.
   */
  public RaftStateMachine setSessionTimeout(long sessionTimeout) {
    if (sessionTimeout <= 0)
      throw new IllegalArgumentException("session timeout must be positive");
    this.sessionTimeout = sessionTimeout;
    return this;
  }

  /**
   * Returns the last index applied to the state machine.
   *
   * @return The last index applied to the state machine.
   */
  public long getLastApplied() {
    return lastApplied;
  }

  /**
   * Sets the last index applied to the state machine.
   *
   * @param lastApplied The last index applied to the state machine.
   */
  private void setLastApplied(long lastApplied) {
    this.lastApplied = lastApplied;
    List<Runnable> queries = this.queries.remove(lastApplied);
    if (queries != null) {
      queries.forEach(Runnable::run);
    }
  }

  /**
   * Filters an entry.
   *
   * @param entry The entry to filter.
   * @return A boolean value indicating whether to keep the entry.
   */
  public CompletableFuture<Boolean> filter(Entry entry, Compaction compaction) {
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
   * @return A boolean value indicating whether to keep the entry.
   */
  public CompletableFuture<Boolean> filter(RegisterEntry entry, Compaction compaction) {
    return CompletableFuture.completedFuture(sessions.containsKey(entry.getIndex()));
  }

  /**
   * Filters an entry.
   *
   * @param entry The entry to filter.
   * @return A boolean value indicating whether to keep the entry.
   */
  public CompletableFuture<Boolean> filter(KeepAliveEntry entry, Compaction compaction) {
    return CompletableFuture.completedFuture(sessions.containsKey(entry.getIndex()) && sessions.get(entry.getIndex()).index == entry.getIndex());
  }

  /**
   * Filters a no-op entry.
   *
   * @param entry The entry to filter.
   * @return A boolean value indicating whether to keep the entry.
   */
  public CompletableFuture<Boolean> filter(NoOpEntry entry, Compaction compaction) {
    return CompletableFuture.completedFuture(false);
  }

  /**
   * Filters an entry.
   *
   * @param entry The entry to filter.
   * @return A boolean value indicating whether to keep the entry.
   */
  public CompletableFuture<Boolean> filter(CommandEntry entry, Compaction compaction) {
    RaftSession session = sessions.get(entry.getSession());
    if (session == null) {
      session = new RaftSession(entry.getSession(), entry.getTimestamp());
      session.expire();
    }
    Commit<? extends Command> commit = new Commit<>(entry.getIndex(), session, entry.getTimestamp(), entry.getCommand());
    return CompletableFuture.supplyAsync(() -> stateMachine.filter(commit, compaction), context);
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  public CompletableFuture<?> apply(Entry entry) {
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
    } else if (entry instanceof JoinEntry) {
      return apply((JoinEntry) entry);
    } else if (entry instanceof LeaveEntry) {
      return apply((LeaveEntry) entry);
    } else if (entry instanceof HeartbeatEntry) {
      return apply((HeartbeatEntry) entry);
    }
    return Futures.exceptionalFuture(new InternalException("unknown state machine operation"));
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  public CompletableFuture<Long> apply(RegisterEntry entry) {
    return register(entry.getIndex(), entry.getTimestamp());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   */
  public CompletableFuture<Void> apply(KeepAliveEntry entry) {
    return keepAlive(entry.getIndex(), entry.getTimestamp(), entry.getSession());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  public CompletableFuture<Object> apply(CommandEntry entry) {
    return command(entry.getIndex(), entry.getSession(), entry.getRequest(), entry.getResponse(), entry.getTimestamp(), entry.getCommand());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  public CompletableFuture<Object> apply(QueryEntry entry) {
    return query(entry.getIndex(), entry.getSession(), entry.getVersion(), entry.getTimestamp(), entry.getQuery());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  public CompletableFuture<Long> apply(NoOpEntry entry) {
    return noop(entry.getIndex());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  public CompletableFuture<Long> apply(JoinEntry entry) {
    return join(entry.getIndex(), entry.getMember());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  public CompletableFuture<Void> apply(LeaveEntry entry) {
    return leave(entry.getIndex(), entry.getMember());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  public CompletableFuture<Void> apply(HeartbeatEntry entry) {
    return heartbeat(entry.getIndex(), entry.getMemberId(), entry.getTimestamp());
  }

  /**
   * Registers a member session.
   *
   * @param index The registration index.
   * @param timestamp The registration timestamp.
   * @return The session ID.
   */
  private CompletableFuture<Long> register(long index, long timestamp) {
    RaftSession session = new RaftSession(index, timestamp);
    sessions.put(index, session);

    // We need to ensure that the command is applied to the state machine before queries are run.
    // Set last applied only after the operation has been submitted to the state machine executor.
    CompletableFuture<Long> future = CompletableFuture.supplyAsync(() -> {
      stateMachine.register(session);
      return session.id();
    }, context);

    setLastApplied(index);
    return future;
  }

  /**
   * Keeps a member session alive.
   *
   * @param index The keep alive index.
   * @param timestamp The keep alive timestamp.
   * @param sessionId The session to keep alive.
   */
  private CompletableFuture<Void> keepAlive(long index, long timestamp, long sessionId) {
    RaftSession session = sessions.get(sessionId);

    // We need to ensure that the command is applied to the state machine before queries are run.
    // Set last applied only after the operation has been submitted to the state machine executor.
    CompletableFuture<Void> future;
    if (session == null) {
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session: " + sessionId));
    } else if (!session.update(index, timestamp)) {
      sessions.remove(sessionId);
      context.execute(() -> stateMachine.expire(session));
      future = Futures.exceptionalFuture(new UnknownSessionException("session expired: " + sessionId));
    } else {
      future = CompletableFuture.runAsync(() -> {}, context);
    }

    setLastApplied(index);
    return future;
  }

  /**
   * Applies a no-op to the state machine.
   *
   * @param index The no-op index.
   * @return The no-op index.
   */
  private CompletableFuture<Long> noop(long index) {
    // We need to ensure that the command is applied to the state machine before queries are run.
    // Set last applied only after the operation has been submitted to the state machine executor.
    CompletableFuture<Long> future = CompletableFuture.supplyAsync(() -> index, context);
    setLastApplied(index);
    return future;
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
   * @return The command result.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Object> command(long index, long sessionId, long request, long response, long timestamp, Command command) {
    CompletableFuture<Object> future;

    // First check to ensure that the session exists.
    RaftSession session = sessions.get(sessionId);
    if (session == null) {
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session " + sessionId));
    } else if (!session.update(index, timestamp)) {
      sessions.remove(sessionId);
      context.execute(() -> stateMachine.expire(session));
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session " + sessionId));
    } else if (session.responses.containsKey(request)) {
      future = CompletableFuture.completedFuture(session.responses.get(request));
    } else {
      // Apply the command to the state machine.
      future = CompletableFuture.supplyAsync(() -> stateMachine.apply(new Commit(index, session, timestamp, command)), context)
        .thenApply(result -> {
          // Store the command result in the session.
          session.responses.put(request, result);

          // Clear any responses that have been received by the client for the session.
          session.responses.headMap(response, true).clear();
          return result;
        });
    }

    // We need to ensure that the command is applied to the state machine before queries are run.
    // Set last applied only after the operation has been submitted to the state machine executor.
    setLastApplied(index);
    return future;
  }

  /**
   * Applies a query to the state machine.
   *
   * @param index The query index.
   * @param sessionId The query session ID.
   * @param version The request version.
   * @param timestamp The query timestamp.
   * @param query The query to apply.
   * @return The query result.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Object> query(long index, long sessionId, long version, long timestamp, Query query) {
    // If the session has not yet been opened or if the client provided a version greater than the last applied index
    // then wait until the up-to-date index is applied to the state machine.
    if (sessionId > lastApplied || version > lastApplied) {
      CompletableFuture<Object> future = new CompletableFuture<>();
      List<Runnable> queries = this.queries.computeIfAbsent(Math.max(sessionId, version), id -> new ArrayList<>());
      queries.add(() -> CompletableFuture.supplyAsync(() -> stateMachine.apply(new Commit(index, sessions.get(sessionId), timestamp, query)), context).whenComplete((result, error) -> {
        if (error == null) {
          future.complete(result);
        } else {
          future.completeExceptionally((Throwable) error);
        }
      }));
      return future;
    } else {
      // Verify that the client's session is still alive.
      RaftSession session = sessions.get(sessionId);
      if (session == null) {
        return Futures.exceptionalFuture(new UnknownSessionException("unknown session: " + sessionId));
      } else if (!session.expire(timestamp)) {
        context.execute(() -> stateMachine.expire(session));
        return Futures.exceptionalFuture(new UnknownSessionException("unknown session: " + sessionId));
      } else {
        return CompletableFuture.supplyAsync(() -> stateMachine.apply(new Commit(index, session, timestamp, query)), context);
      }
    }
  }

  /**
   * Applies a join to the state machine.
   *
   * @param index The join index.
   * @param member The joining member.
   * @return A completable future to be completed with the join index.
   */
  private CompletableFuture<Long> join(long index, MemberInfo member) {
    CompletableFuture<Long> future = cluster.addMember(member).thenApplyAsync(v -> index, context);
    setLastApplied(index);
    return future;
  }

  /**
   * Applies a leave to the state machine.
   *
   * @param index The leave index.
   * @param member The leaving member.
   * @return A completable future to be completed once the member has been removed.
   */
  private CompletableFuture<Void> leave(long index, MemberInfo member) {
    CompletableFuture<Void> future = cluster.removeMember(member.id());
    setLastApplied(index);
    return future;
  }

  /**
   * Applies a heartbeat to the state machine.
   *
   * @param index The heartbeat index.
   * @param memberId The member ID.
   * @param timestamp The heartbeat timestamp.
   * @return A completable future to be completed once the heartbeat has been applied.
   */
  private CompletableFuture<Void> heartbeat(long index, int memberId, long timestamp) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    ManagedMember clusterMember = cluster.member(memberId);
    if (clusterMember == null) {
      future.completeExceptionally(new IllegalStateException("unknown cluster member: " + memberId));
    } else {
      MemberState member = members.getMember(memberId);
      if (member == null) {
        members.addMember(new MemberState(memberId, clusterMember.type(), timestamp).setVersion(index));
        cluster.configureMember(memberId, Member.Status.ALIVE);
      } else {
        member.update(timestamp, sessionTimeout);
      }
      future.complete(null);
    }

    Iterator<MemberState> iterator = members.iterator();
    while (iterator.hasNext()) {
      MemberState member = iterator.next();
      if (!member.update(timestamp, sessionTimeout) && member.getType() != Member.Type.ACTIVE) {
        iterator.remove();
        cluster.configureMember(member.getId(), Member.Status.DEAD);
      }
    }

    setLastApplied(index);
    return future;
  }

  /**
   * Cluster session.
   */
  private class RaftSession extends Session {
    private long index;
    private long timestamp;
    private final TreeMap<Long, Object> responses = new TreeMap<>();

    private RaftSession(long id, long timestamp) {
      super(id);
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
    private boolean expire(long timestamp) {
      if (timestamp - sessionTimeout > this.timestamp) {
        expire();
        return false;
      }
      this.timestamp = timestamp;
      return true;
    }

    /**
     * Updates the session.
     *
     * @param timestamp The session.
     */
    private boolean update(long index, long timestamp) {
      if (timestamp - sessionTimeout > this.timestamp) {
        expire();
        return false;
      }
      this.index = index;
      this.timestamp = timestamp;
      return true;
    }

  }

}
