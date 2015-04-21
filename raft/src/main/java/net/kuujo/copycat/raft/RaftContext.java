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

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.raft.protocol.ProtocolHandler;
import net.kuujo.copycat.raft.protocol.Request;
import net.kuujo.copycat.raft.protocol.Response;
import net.kuujo.copycat.raft.storage.RaftStorage;
import net.kuujo.copycat.util.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Raft state context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class RaftContext extends Observable implements Managed<RaftContext> {
  private final Logger LOGGER = LoggerFactory.getLogger(RaftContext.class);
  private final ScheduledExecutorService executor;
  private Thread thread;
  private final RaftConfig config;
  private RaftStorage log;
  private RaftState state;
  private CompletableFuture<RaftContext> openFuture;
  private RaftMember localMember;
  private final Map<Integer, RaftMember> members = new HashMap<>();
  private boolean recovering = true;
  private int leader;
  private long term;
  private long version;
  private int lastVotedFor;
  private long firstCommitIndex = 0;
  private long commitIndex = 0;
  private long recycleIndex = 0;
  private long lastApplied = 0;
  private long electionTimeout = 500;
  private long heartbeatInterval = 250;
  private volatile boolean open;

  protected RaftContext(RaftStorage log, RaftConfig config, ScheduledExecutorService executor) {
    this.log = log;
    this.config = config;
    this.executor = executor;
    try {
      executor.submit(() -> this.thread = Thread.currentThread()).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to initialize thread state", e);
    }
  }

  /**
   * Returns the Raft configuration.
   *
   * @return The Raft configuration.
   */
  protected RaftConfig getConfig() {
    return config;
  }

  /**
   * Returns the local member.
   *
   * @return The local member.
   */
  protected RaftMember getLocalMember() {
    return localMember;
  }

  /**
   * Returns a collection of all members in the cluster.
   *
   * @return A collection of all members in the cluster.
   */
  protected Collection<RaftMember> getMembers() {
    return members.values();
  }

  /**
   * Returns member info for a specific member.
   *
   * @param id The id of the member for which to return member info.
   * @return The Raft member.
   */
  protected RaftMember getMember(int id) {
    return members.get(id);
  }

  /**
   * Sets the set of active and promotable members.
   *
   * @param members A collection of active and promotable members.
   * @return The Raft context.
   */
  RaftContext setMembers(Collection<RaftMember> members) {
    Iterator<Map.Entry<Integer, RaftMember>> iterator = this.members.entrySet().iterator();
    while (iterator.hasNext()) {
      RaftMember member = iterator.next().getValue();
      if (member.type() == RaftMember.Type.ACTIVE && !members.contains(member)) {
        iterator.remove();
      }
    }
    members.forEach(this::addMember);
    triggerChangeEvent();
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
  protected RaftContext addMember(RaftMember member) {
    RaftMember m = members.get(member.id());
    if (m != null) {
      if (m.update(member)) {
        triggerChangeEvent();
      }
    } else {
      members.put(member.id(), member);
    }
    return this;
  }

  /**
   * Removes a member from the cluster.
   *
   * @param member The member to remove.
   * @return The Raft context.
   */
  protected RaftContext removeMember(RaftMember member) {
    members.remove(member.id());
    return this;
  }

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   * @return The Raft context.
   */
  RaftContext setLeader(int leader) {
    if (this.leader == 0) {
      if (leader != 0) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", localMember, leader);
        if (openFuture != null) {
          openFuture.complete(this);
          openFuture = null;
        }
        triggerChangeEvent();
      }
    } else if (leader != 0) {
      if (this.leader != leader) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", localMember, leader);
        triggerChangeEvent();
      }
    } else {
      this.leader = 0;
      triggerChangeEvent();
    }
    return this;
  }

  /**
   * Returns the state leader.
   *
   * @return The state leader.
   */
  protected int getLeader() {
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
      this.leader = 0;
      this.lastVotedFor = 0;
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
  protected long getTerm() {
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
  protected long getVersion() {
    return version;
  }

  /**
   * Returns whether the context is recovering.
   *
   * @return Indicates whether the context is currently recovering.
   */
  protected boolean isRecovering() {
    return recovering;
  }

  /**
   * Sets the state last voted for candidate.
   *
   * @param candidate The candidate that was voted for.
   * @return The Raft context.
   */
  RaftContext setLastVotedFor(int candidate) {
    // If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
    if (lastVotedFor != 0 && candidate != 0) {
      throw new IllegalStateException("Already voted for another candidate");
    }
    if (leader != 0 && candidate != 0) {
      throw new IllegalStateException("Cannot cast vote - leader already exists");
    }
    this.lastVotedFor = candidate;
    if (candidate != 0) {
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
  protected int getLastVotedFor() {
    return lastVotedFor;
  }

  /**
   * Sets the commit index.
   *
   * @param commitIndex The commit index.
   * @return The Raft context.
   */
  RaftContext setCommitIndex(long commitIndex) {
    if (commitIndex < 0)
      throw new IllegalArgumentException("commit index must be positive");
    if (commitIndex < this.commitIndex)
      throw new IllegalArgumentException("cannot decrease commit index");
    if (firstCommitIndex == 0)
      firstCommitIndex = commitIndex;
    this.commitIndex = commitIndex;
    localMember.commitIndex(this.commitIndex);
    return this;
  }

  /**
   * Returns the commit index.
   *
   * @return The commit index.
   */
  protected long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Sets the recycle index.
   *
   * @param recycleIndex The recycle index.
   * @return The Raft context.
   */
  RaftContext setRecycleIndex(long recycleIndex) {
    if (recycleIndex < 0)
      throw new IllegalArgumentException("recycle index must be positive");
    if (recycleIndex < this.recycleIndex)
      throw new IllegalArgumentException("cannot decrease recycle index");
    this.recycleIndex = recycleIndex;
    localMember.recycleIndex(recycleIndex);
    return this;
  }

  /**
   * Returns the recycle index.
   *
   * @return The state recycle index.
   */
  protected long getRecycleIndex() {
    return recycleIndex;
  }

  /**
   * Sets the state last applied index.
   *
   * @param lastApplied The state last applied index.
   * @return The Raft context.
   */
  RaftContext setLastApplied(long lastApplied) {
    if (lastApplied < 0)
      throw new IllegalArgumentException("last applied must be positive");
    if (lastApplied < this.lastApplied)
      throw new IllegalArgumentException("cannot decrease last applied");
    if (lastApplied > commitIndex)
      throw new IllegalArgumentException("last applied cannot be greater than commit index");
    this.lastApplied = lastApplied;
    if (recovering && this.lastApplied != 0 && firstCommitIndex != 0 && this.lastApplied >= firstCommitIndex) {
      recovering = false;
    }
    return this;
  }

  /**
   * Returns the state last applied index.
   *
   * @return The state last applied index.
   */
  protected long getLastApplied() {
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
  protected long getElectionTimeout() {
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
  protected long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Returns the context executor.
   *
   * @return The context executor.
   */
  ScheduledExecutorService executor() {
    return executor;
  }

  /**
   * Commits an entry to the context.
   *
   * @param key The entry key.
   * @param entry The entry value.
   * @param result The buffer to which to write the commit result.
   * @return The result buffer.
   */
  protected abstract Buffer commit(Buffer key, Buffer entry, Buffer result);

  /**
   * Returns the state log.
   *
   * @return The state log.
   */
  RaftStorage log() {
    return log;
  }

  /**
   * Applies a request to the local state.
   *
   * @param request The request to apply.
   * @return The request response.
   */
  public CompletableFuture<? extends Response> apply(Request request) {
    return wrapCall(request, state);
  }

  /**
   * Sends a request.
   *
   * @param request The request to send.
   * @param member The member to which to send the request.
   * @param <T> The request type.
   * @param <U> The expected response type.
   * @return A completable future to be completed with the response.
   */
  protected abstract <T extends Request, U extends Response> CompletableFuture<U> sendRequest(T request, RaftMember member);

  /**
   * Handles a request.
   *
   * @param request The request to handle.
   * @return A completable future to be called with the response.
   */
  protected CompletableFuture<Response> handleRequest(Request request) {
    return wrapCall(request, state);
  }

  /**
   * Wraps a call to the state context in the context executor.
   */
  private <T extends Request, U extends Response> CompletableFuture<U> wrapCall(T request, ProtocolHandler<T, U> handler) {
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
   * Transition handler.
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
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    } else {
      // Force state transitions to occur synchronously in order to prevent race conditions.
      try {
        this.state = state.type().getConstructor(RaftContext.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    }
    return CompletableFuture.completedFuture(null);
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
  public synchronized CompletableFuture<RaftContext> open() {
    if (openFuture != null) {
      return openFuture;
    }

    openFuture = new CompletableFuture<>();

    this.localMember = new RaftMember(config.getMemberId(), config.getMemberType());
    members.put(localMember.id(), localMember);
    config.getMembers().forEach(id -> {
      members.put(id, new RaftMember(id, RaftMember.Type.ACTIVE));
    });
    this.electionTimeout = config.getElectionTimeout();
    this.heartbeatInterval = config.getHeartbeatInterval();

    executor.execute(() -> {
      try {
        open = true;
        log.open();
        switch (localMember.type()) {
          case REMOTE:
            transition(RaftState.Type.REMOTE);
            break;
          case PASSIVE:
            transition(RaftState.Type.PASSIVE);
            break;
          case ACTIVE:
            transition(RaftState.Type.FOLLOWER);
            break;
        }
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
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalStateException("Context not open"));
      return future;
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    executor.execute(() -> {
      transition(RaftState.Type.START).whenComplete((result, error) -> {
        if (error == null) {
          try {
            log.close();
            localMember = null;
            members.clear();
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
