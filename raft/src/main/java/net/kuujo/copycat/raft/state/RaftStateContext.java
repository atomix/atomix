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
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.NoLeaderException;
import net.kuujo.copycat.raft.Query;
import net.kuujo.copycat.raft.RaftError;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.log.Compactor;
import net.kuujo.copycat.raft.log.Log;
import net.kuujo.copycat.raft.rpc.*;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.ThreadChecker;
import net.kuujo.copycat.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftStateContext extends RaftStateClient {
  private final Logger LOGGER = LoggerFactory.getLogger(RaftStateContext.class);
  private final RaftStateMachine stateMachine;
  private final Log log;
  private final Compactor compactor;
  private final ManagedCluster cluster;
  private final ClusterState members = new ClusterState();
  private final ExecutionContext context;
  private final ThreadChecker threadChecker;
  private AbstractState state;
  private ScheduledFuture<?> joinTimer;
  private ScheduledFuture<?> heartbeatTimer;
  private final AtomicBoolean heartbeat = new AtomicBoolean();
  private final Random random = new Random();
  private long electionTimeout = 500;
  private long heartbeatInterval = 250;
  private int lastVotedFor;
  private long commitIndex;
  private long globalIndex;
  private long lastApplied;
  private volatile boolean open;

  public RaftStateContext(Log log, StateMachine stateMachine, ManagedCluster cluster, ExecutionContext context) {
    super(cluster, new ExecutionContext(String.format("%s-client", context.name()), context.serializer().copy()));
    this.log = log;
    this.stateMachine = new RaftStateMachine(stateMachine, cluster, members, new ExecutionContext(String.format("%s-state", context.name()), context.serializer().copy()));
    this.compactor = new Compactor(log, this.stateMachine::filter, context);
    this.cluster = cluster;
    this.context = context;
    this.threadChecker = new ThreadChecker(context);
  }

  /**
   * Returns the Raft cluster.
   *
   * @return The Raft cluster.
   */
  public ManagedCluster getCluster() {
    return cluster;
  }

  /**
   * Returns the command serializer.
   *
   * @return The command serializer.
   */
  public Serializer getSerializer() {
    return cluster.serializer();
  }

  /**
   * Returns the execution context.
   *
   * @return The execution context.
   */
  public ExecutionContext getContext() {
    return context;
  }

  /**
   * Sets the election timeout.
   *
   * @param electionTimeout The election timeout.
   * @return The Raft context.
   */
  public RaftStateContext setElectionTimeout(long electionTimeout) {
    this.electionTimeout = electionTimeout;
    return this;
  }

  /**
   * Returns the election timeout.
   *
   * @return The election timeout.
   */
  public long getElectionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the heartbeat interval.
   *
   * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
   * @return The Raft context.
   */
  public RaftStateContext setHeartbeatInterval(long heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
    return this;
  }

  /**
   * Returns the heartbeat interval.
   *
   * @return The heartbeat interval in milliseconds.
   */
  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Sets the session timeout.
   *
   * @param sessionTimeout The session timeout in milliseconds.
   * @return The Raft context.
   */
  public RaftStateContext setSessionTimeout(long sessionTimeout) {
    stateMachine.setSessionTimeout(sessionTimeout);
    return this;
  }

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   * @return The Raft context.
   */
  RaftStateContext setLeader(int leader) {
    if (this.leader == 0) {
      if (leader != 0) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", cluster.member().id(), leader);
      }
    } else if (leader != 0) {
      if (this.leader != leader) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", cluster.member().id(), leader);
      }
    } else {
      this.leader = 0;
    }
    return this;
  }

  /**
   * Returns the cluster state.
   *
   * @return The cluster state.
   */
  ClusterState getMembers() {
    return members;
  }

  /**
   * Returns the state leader.
   *
   * @return The state leader.
   */
  public int getLeader() {
    return leader;
  }

  /**
   * Sets the state term.
   *
   * @param term The state term.
   * @return The Raft context.
   */
  RaftStateContext setTerm(long term) {
    if (term > this.term) {
      this.term = term;
      this.leader = 0;
      this.lastVotedFor = 0;
      LOGGER.debug("{} - Incremented term {}", cluster.member().id(), term);
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
   * Sets the state last voted for candidate.
   *
   * @param candidate The candidate that was voted for.
   * @return The Raft context.
   */
  RaftStateContext setLastVotedFor(int candidate) {
    // If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
    if (lastVotedFor != 0 && candidate != 0) {
      throw new IllegalStateException("Already voted for another candidate");
    }
    if (leader != 0 && candidate != 0) {
      throw new IllegalStateException("Cannot cast vote - leader already exists");
    }
    this.lastVotedFor = candidate;
    if (candidate != 0) {
      LOGGER.debug("{} - Voted for {}", cluster.member().id(), candidate);
    } else {
      LOGGER.debug("{} - Reset last voted for", cluster.member().id());
    }
    return this;
  }

  /**
   * Returns the state last voted for candidate.
   *
   * @return The state last voted for candidate.
   */
  public int getLastVotedFor() {
    return lastVotedFor;
  }

  /**
   * Sets the commit index.
   *
   * @param commitIndex The commit index.
   * @return The Raft context.
   */
  RaftStateContext setCommitIndex(long commitIndex) {
    if (commitIndex < 0)
      throw new IllegalArgumentException("commit index must be positive");
    if (commitIndex < this.commitIndex)
      throw new IllegalArgumentException("cannot decrease commit index");
    this.commitIndex = commitIndex;
    return this;
  }

  /**
   * Returns the commit index.
   *
   * @return The commit index.
   */
  public long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Sets the recycle index.
   *
   * @param globalIndex The recycle index.
   * @return The Raft context.
   */
  RaftStateContext setGlobalIndex(long globalIndex) {
    if (globalIndex < 0)
      throw new IllegalArgumentException("global index must be positive");
    this.globalIndex = Math.max(this.globalIndex, globalIndex);
    compactor.setCompactIndex(globalIndex);
    return this;
  }

  /**
   * Returns the recycle index.
   *
   * @return The state recycle index.
   */
  public long getGlobalIndex() {
    return globalIndex;
  }

  /**
   * Sets the state last applied index.
   *
   * @param lastApplied The state last applied index.
   * @return The Raft context.
   */
  RaftStateContext setLastApplied(long lastApplied) {
    if (lastApplied < 0)
      throw new IllegalArgumentException("last applied must be positive");
    if (lastApplied < this.lastApplied)
      throw new IllegalArgumentException("cannot decrease last applied");
    if (lastApplied > commitIndex)
      throw new IllegalArgumentException("last applied cannot be greater than commit index");
    this.lastApplied = lastApplied;
    compactor.setCommitIndex(lastApplied);
    return this;
  }

  /**
   * Returns the state last applied index.
   *
   * @return The state last applied index.
   */
  public long getLastApplied() {
    return lastApplied;
  }

  /**
   * Returns the current state.
   *
   * @return The current state.
   */
  public RaftState getState() {
    return state.type();
  }

  /**
   * Returns the state machine proxy.
   *
   * @return The state machine proxy.
   */
  RaftStateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * Returns the state log.
   *
   * @return The state log.
   */
  public Log getLog() {
    return log;
  }

  /**
   * Checks that the current thread is the state context thread.
   */
  void checkThread() {
    threadChecker.checkThread();
  }

  @Override
  protected Member selectMember(Query<?> query) {
    if (!query.consistency().isLeaderRequired()) {
      return cluster.member();
    }
    return super.selectMember(query);
  }

  @Override
  protected CompletableFuture<Void> register(List<Member> members) {
    return register(members, new CompletableFuture<>()).thenAccept(response -> {
      setSession(response.session());
    });
  }

  @Override
  protected CompletableFuture<Void> keepAlive(List<Member> members) {
    return keepAlive(members, new CompletableFuture<>()).thenAccept(response -> {
      setVersion(response.version());
    });
  }

  /**
   * Transition handler.
   */
  CompletableFuture<RaftState> transition(Class<? extends AbstractState> state) {
    checkThread();

    if (this.state != null && state == this.state.getClass()) {
      return CompletableFuture.completedFuture(this.state.type());
    }

    LOGGER.info("{} - Transitioning to {}", cluster.member().id(), state.getSimpleName());

    // Force state transitions to occur synchronously in order to prevent race conditions.
    if (this.state != null) {
      try {
        this.state.close().get();
        this.state = state.getConstructor(RaftStateContext.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    } else {
      // Force state transitions to occur synchronously in order to prevent race conditions.
      try {
        this.state = state.getConstructor(RaftStateContext.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Joins the cluster.
   */
  private CompletableFuture<Void> join() {
    return join(100, new CompletableFuture<>());
  }

  /**
   * Joins the cluster.
   */
  private CompletableFuture<Void> join(long interval, CompletableFuture<Void> future) {
    join(new ArrayList<>(cluster.members()), new CompletableFuture<>()).whenComplete((result, error) -> {
      if (error == null) {
        future.complete(null);
      } else {
        long nextInterval = Math.min(interval * 2, 5000);
        joinTimer = context.schedule(() -> join(nextInterval, future), nextInterval, TimeUnit.MILLISECONDS);
      }
    });
    return future;
  }

  /**
   * Joins the cluster by contacting a random member.
   */
  private CompletableFuture<Void> join(List<Member> members, CompletableFuture<Void> future) {
    if (members.isEmpty()) {
      future.completeExceptionally(new NoLeaderException("no leader found"));
      return future;
    }
    return join(selectMember(members), members, future);
  }

  /**
   * Sends a join request to a specific member.
   */
  private CompletableFuture<Void> join(Member member, List<Member> members, CompletableFuture<Void> future) {
    LOGGER.debug("{} - Joining cluster via {}", cluster.member().id(), member.id());
    JoinRequest request = JoinRequest.builder()
      .withMember(cluster.member().info())
      .build();
    member.<JoinRequest, JoinResponse>send(request).whenComplete((response, error) -> {
      threadChecker.checkThread();
      if (error == null && response.status() == Response.Status.OK) {
        setLeader(response.leader());
        setTerm(response.term());
        future.complete(null);
        LOGGER.info("{} - Joined cluster", cluster.member().id());
      } else {
        if (member.id() == getLeader()) {
          setLeader(0);
        }
        LOGGER.debug("Cluster join failed, retrying");
        setLeader(0);
        join(members, future);
      }
    });
    return future;
  }

  /**
   * Starts the heartbeat timer.
   */
  private void startHeartbeatTimer() {
    LOGGER.debug("Starting keep alive timer");
    heartbeatTimer = context.scheduleAtFixedRate(this::heartbeat, 1, heartbeatInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Sends a heartbeat to the leader.
   */
  private void heartbeat() {
    if (heartbeat.compareAndSet(false, true)) {
      LOGGER.debug("{} - Sending heartbeat request", cluster.member().id());
      heartbeat(cluster.members().stream()
        .filter(m -> m.type() == Member.Type.ACTIVE)
        .collect(Collectors.toList()), new CompletableFuture<>()).thenRun(() -> heartbeat.set(false));
    }
  }

  /**
   * Sends a heartbeat to a random member.
   */
  private CompletableFuture<Void> heartbeat(List<Member> members, CompletableFuture<Void> future) {
    if (members.isEmpty()) {
      future.completeExceptionally(RaftError.Type.NO_LEADER_ERROR.createException());
      heartbeat.set(false);
      return future;
    }
    return heartbeat(selectMember(members), members, future);
  }

  /**
   * Sends a heartbeat to a specific member.
   */
  private CompletableFuture<Void> heartbeat(Member member, List<Member> members, CompletableFuture<Void> future) {
    KeepAliveRequest request = KeepAliveRequest.builder()
      .withSession(getSession())
      .build();
    member.<KeepAliveRequest, KeepAliveResponse>send(request).whenComplete((response, error) -> {
      threadChecker.checkThread();
      if (isOpen()) {
        if (error == null && response.status() == Response.Status.OK) {
          setLeader(response.leader());
          setTerm(response.term());
          future.complete(null);
        } else {
          if (member.id() == getLeader()) {
            setLeader(0);
          }
          heartbeat(members, future);
        }
      }
    });
    return future;
  }

  /**
   * Leaves the cluster.
   */
  private CompletableFuture<Void> leave() {
    return leave(cluster.members().stream()
      .filter(m -> m.type() == Member.Type.ACTIVE)
      .collect(Collectors.toList()), new CompletableFuture<>());
  }

  /**
   * Leaves the cluster by contacting a random member.
   */
  private CompletableFuture<Void> leave(List<Member> members, CompletableFuture<Void> future) {
    if (members.isEmpty()) {
      future.completeExceptionally(new NoLeaderException("no leader found"));
      return future;
    }
    return leave(selectMember(members), members, future);
  }

  /**
   * Sends a leave request to a specific member.
   */
  private CompletableFuture<Void> leave(Member member, List<Member> members, CompletableFuture<Void> future) {
    LOGGER.debug("{} - Leaving cluster via {}", cluster.member().id(), member.id());
    LeaveRequest request = LeaveRequest.builder()
      .withMember(cluster.member().info())
      .build();
    member.<LeaveRequest, LeaveResponse>send(request).whenComplete((response, error) -> {
      threadChecker.checkThread();
      if (error == null && response.status() == Response.Status.OK) {
        future.complete(null);
        LOGGER.info("{} - Left cluster", cluster.member().id());
      } else {
        if (member.id() == getLeader()) {
          setLeader(0);
        }
        LOGGER.debug("Cluster leave failed, retrying");
        setLeader(0);
        leave(members, future);
      }
    });
    return future;
  }

  /**
   * Cancels the join timer.
   */
  private void cancelJoinTimer() {
    if (joinTimer != null) {
      LOGGER.debug("cancelling join timer");
      joinTimer.cancel(false);
    }
  }

  /**
   * Cancels the heartbeat timer.
   */
  private void cancelHeartbeatTimer() {
    if (heartbeatTimer != null) {
      LOGGER.debug("cancelling heartbeat timer");
      heartbeatTimer.cancel(false);
    }
  }

  /**
   * Selects a random(ish) member from a members list.
   */
  private Member selectMember(List<Member> members) {
    Member member;
    if (leader != 0) {
      member = cluster.member(leader);
      if (member == null) {
        setLeader(0);
        return members.remove(random.nextInt(members.size()));
      }
      return member;
    } else {
      return members.remove(random.nextInt(members.size()));
    }
  }

  @Override
  public synchronized CompletableFuture<Void> open() {
    if (cluster.member().type() == Member.Type.PASSIVE) {
      return cluster.open().thenRunAsync(() -> {
        log.open(context);
        transition(PassiveState.class);
      }, context)
        .thenComposeAsync(v -> join(), context)
        .thenCompose(v -> super.open())
        .thenRun(() -> {
          startHeartbeatTimer();
          open = true;
        });
    } else {
      return cluster.open().thenRunAsync(() -> {
        log.open(context);
        transition(FollowerState.class);
        open = true;
      }, context)
        .thenCompose(v -> super.open());
    }
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (!open)
      return Futures.exceptionalFuture(new IllegalStateException("context not open"));

    CompletableFuture<Void> future = new CompletableFuture<>();
    context.execute(() -> {
      cancelJoinTimer();
      cancelHeartbeatTimer();
      open = false;
      transition(StartState.class)
        .thenComposeAsync(v -> super.close(), context)
        .thenComposeAsync(v -> leave(), context)
        .thenComposeAsync(v -> cluster.close(), context)
        .whenCompleteAsync((result, error) -> {
          try {
            if (log != null) {
              log.close();
            }
          } catch (Exception e) {
          }

          if (error == null) {
            future.complete(null);
          } else {
            future.completeExceptionally(error);
          }
        }, context);
    });
    return future;
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  /**
   * Deletes the context.
   */
  public CompletableFuture<Void> delete() {
    if (open)
      return Futures.exceptionalFuture(new IllegalStateException("cannot delete open context"));

    return CompletableFuture.runAsync(() -> {
      if (log != null)
        log.delete();
    }, context);
  }

  @Override
  public String toString() {
    return getClass().getCanonicalName();
  }

}
