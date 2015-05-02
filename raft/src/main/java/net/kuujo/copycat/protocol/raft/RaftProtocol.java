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
package net.kuujo.copycat.protocol.raft;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.protocol.raft.rpc.Response;
import net.kuujo.copycat.protocol.raft.rpc.SubmitRequest;
import net.kuujo.copycat.protocol.raft.storage.RaftStorage;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.ThreadChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Raft protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftProtocol extends AbstractProtocol {

  /**
   * Returns a new Raft protocol builder.
   *
   * @return A new Raft protocol builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final Logger LOGGER = LoggerFactory.getLogger(RaftProtocol.class);
  private ThreadChecker threadChecker;
  private final RaftConfig config;
  private RaftStorage storage;
  private RaftState state;
  private final Map<Integer, RaftMember> members = new HashMap<>();
  private CompletableFuture<Protocol> openFuture;
  private CommitHandler commitHandler;
  private boolean recovering = true;
  private int leader;
  private long term;
  private long version;
  private int lastVotedFor;
  private long firstCommitIndex = 0;
  private long commitIndex = 0;
  private long recycleIndex = 0;
  private long lastApplied = 0;
  private volatile boolean open;

  protected RaftProtocol(RaftStorage storage, RaftConfig config) {
    this.storage = storage;
    this.config = config;
  }

  @Override
  public void setContext(ExecutionContext context) {
    super.setContext(context);
    threadChecker = new ThreadChecker(context);
  }

  /**
   * Returns member info for a specific member.
   *
   * @param id The id of the member for which to return member info.
   * @return The Raft member.
   */
  RaftMember getRaftMember(int id) {
    RaftMember member = members.get(id);
    if (member == null) {
      member = new RaftMember(id);
      members.put(id, member);
    }
    return member;
  }

  /**
   * Returns the full collection of Raft members.
   *
   * @return The full collection of Raft members.
   */
  Collection<RaftMember> getRaftMembers() {
    return members.values();
  }

  /**
   * Sets the set of members.
   *
   * @param members A collection of members to set.
   * @return The Raft context.
   */
  RaftProtocol updateMembers(Collection<RaftMember> members) {
    members.forEach(member -> {
      RaftMember m = getRaftMember(member.id());
      if (m != null) {
        m.update(member);
      } else {
        this.members.put(member.id(), member);
      }
    });
    return this;
  }

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   * @return The Raft context.
   */
  RaftProtocol setLeader(int leader) {
    if (this.leader == 0) {
      if (leader != 0) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", cluster.member().id(), leader);
        if (openFuture != null) {
          openFuture.complete(this);
          openFuture = null;
        }
        listeners.forEach(l -> l.accept(new LeaderChangeEvent(null, cluster.member(leader))));
      }
    } else if (leader != 0) {
      if (this.leader != leader) {
        Member oldLeader = cluster.member(this.leader);
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", cluster.member().id(), leader);
        listeners.forEach(l -> l.accept(new LeaderChangeEvent(oldLeader, cluster.member(leader))));
      }
    } else {
      Member oldLeader = cluster.member(this.leader);
      this.leader = 0;
      listeners.forEach(l -> l.accept(new LeaderChangeEvent(oldLeader, null)));
    }
    return this;
  }

  /**
   * Returns the state leader.
   *
   * @return The state leader.
   */
  int getLeader() {
    return leader;
  }

  /**
   * Sets the state term.
   *
   * @param term The state term.
   * @return The Raft context.
   */
  RaftProtocol setTerm(long term) {
    if (term > this.term) {
      long oldTerm = this.term;
      this.term = term;
      this.leader = 0;
      this.lastVotedFor = 0;
      LOGGER.debug("{} - Incremented term {}", cluster.member().id(), term);
      listeners.forEach(l -> l.accept(new EpochChangeEvent(oldTerm, term)));
    }
    return this;
  }

  /**
   * Returns the state term.
   *
   * @return The state term.
   */
  long getTerm() {
    return term;
  }

  /**
   * Sets the state version.
   *
   * @param version The state version.
   * @return The Raft context.
   */
  RaftProtocol setVersion(long version) {
    this.version = Math.max(this.version, version);
    getRaftMember(cluster.member().id()).version(this.version);
    return this;
  }

  /**
   * Returns the state version.
   *
   * @return The state version.
   */
  long getVersion() {
    return version;
  }

  /**
   * Sets the state last voted for candidate.
   *
   * @param candidate The candidate that was voted for.
   * @return The Raft context.
   */
  RaftProtocol setLastVotedFor(int candidate) {
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
  int getLastVotedFor() {
    return lastVotedFor;
  }

  /**
   * Sets the commit index.
   *
   * @param commitIndex The commit index.
   * @return The Raft context.
   */
  RaftProtocol setCommitIndex(long commitIndex) {
    if (commitIndex < 0)
      throw new IllegalArgumentException("commit index must be positive");
    if (commitIndex < this.commitIndex)
      throw new IllegalArgumentException("cannot decrease commit index");
    if (firstCommitIndex == 0) {
      if (commitIndex == 0) {
        if (recovering) {
          recovering = false;
          listeners.forEach(l -> l.accept(new StatusChangeEvent(Status.RECOVERING, Status.HEALTHY)));
        }
      } else {
        firstCommitIndex = commitIndex;
      }
    }
    this.commitIndex = commitIndex;
    getRaftMember(cluster.member().id()).commitIndex(commitIndex);
    return this;
  }

  /**
   * Returns the commit index.
   *
   * @return The commit index.
   */
  long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Sets the recycle index.
   *
   * @param recycleIndex The recycle index.
   * @return The Raft context.
   */
  RaftProtocol setRecycleIndex(long recycleIndex) {
    if (recycleIndex < 0)
      throw new IllegalArgumentException("recycle index must be positive");
    if (recycleIndex < this.recycleIndex)
      throw new IllegalArgumentException("cannot decrease recycle index");
    this.recycleIndex = recycleIndex;
    getRaftMember(cluster.member().id()).recycleIndex(recycleIndex);
    return this;
  }

  /**
   * Returns the recycle index.
   *
   * @return The state recycle index.
   */
  long getRecycleIndex() {
    return recycleIndex;
  }

  /**
   * Sets the state last applied index.
   *
   * @param lastApplied The state last applied index.
   * @return The Raft context.
   */
  RaftProtocol setLastApplied(long lastApplied) {
    if (lastApplied < 0)
      throw new IllegalArgumentException("last applied must be positive");
    if (lastApplied < this.lastApplied)
      throw new IllegalArgumentException("cannot decrease last applied");
    if (lastApplied > commitIndex)
      throw new IllegalArgumentException("last applied cannot be greater than commit index");
    this.lastApplied = lastApplied;
    if (recovering && this.lastApplied != 0 && firstCommitIndex != 0 && this.lastApplied >= firstCommitIndex) {
      recovering = false;
      listeners.forEach(l -> l.accept(new StatusChangeEvent(Status.RECOVERING, Status.HEALTHY)));
    }
    return this;
  }

  /**
   * Returns the state last applied index.
   *
   * @return The state last applied index.
   */
  long getLastApplied() {
    return lastApplied;
  }

  /**
   * Returns the state election timeout.
   *
   * @return The state election timeout.
   */
  long getElectionTimeout() {
    return config.getElectionTimeout();
  }

  /**
   * Returns the state heartbeat interval.
   *
   * @return The state heartbeat interval.
   */
  long getHeartbeatInterval() {
    return config.getHeartbeatInterval();
  }

  @Override
  public Protocol handler(CommitHandler handler) {
    this.commitHandler = handler;
    return this;
  }

  /**
   * Commits an entry to the context.
   *
   * @param key The entry key.
   * @param entry The entry value.
   * @param result The buffer to which to write the commit result.
   * @return The result buffer.
   */
  Buffer commit(Buffer key, Buffer entry, Buffer result) {
    if (commitHandler != null) {
      return commitHandler.apply(key, entry, result);
    }
    return result;
  }

  /**
   * Returns the state log.
   *
   * @return The state log.
   */
  RaftStorage log() {
    return storage;
  }

  /**
   * Checks that the current thread is the state context thread.
   */
  void checkThread() {
    threadChecker.checkThread();
  }

  @Override
  public CompletableFuture<Buffer> submit(Buffer key, Buffer entry, Persistence persistence, Consistency consistency) {
    if (!open)
      throw new IllegalStateException("protocol not open");

    CompletableFuture<Buffer> future = new CompletableFuture<>();
    SubmitRequest request = SubmitRequest.builder()
      .withKey(key)
      .withEntry(entry)
      .withPersistence(persistence)
      .withConsistency(consistency)
      .build();
    context.execute(() -> {
      state.submit(request).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status() == Response.Status.OK) {
            future.complete(response.result());
          } else {
            future.completeExceptionally(response.error().createException());
          }
        } else {
          future.completeExceptionally(error);
        }
        request.close();
      });
    });
    return future;
  }

  /**
   * Transition handler.
   */
  CompletableFuture<RaftState.Type> transition(RaftState.Type state) {
    checkThread();

    if (this.state != null && state == this.state.type()) {
      return CompletableFuture.completedFuture(this.state.type());
    }

    LOGGER.info("{} - Transitioning to {}", cluster.member().id(), state);

    // Force state transitions to occur synchronously in order to prevent race conditions.
    if (this.state != null) {
      try {
        this.state.close().get();
        this.state = state.type().getConstructor(RaftProtocol.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    } else {
      // Force state transitions to occur synchronously in order to prevent race conditions.
      try {
        this.state = state.type().getConstructor(RaftProtocol.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public synchronized CompletableFuture<Protocol> open() {
    if (openFuture != null) {
      return openFuture;
    }

    openFuture = new CompletableFuture<>();

    context.execute(() -> {
      try {
        open = true;
        switch (cluster.member().type()) {
          case REMOTE:
            transition(RaftState.Type.REMOTE);
            break;
          case PASSIVE:
            storage.open();
            transition(RaftState.Type.PASSIVE);
            break;
          case ACTIVE:
            storage.open();
            transition(RaftState.Type.FOLLOWER);
            break;
        }
        listeners.forEach(l -> l.accept(new StatusChangeEvent(null, Status.RECOVERING)));
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
    context.execute(() -> {
      transition(RaftState.Type.START).whenComplete((result, error) -> {
        if (error == null) {
          try {
            storage.close();
            future.complete(null);
          } catch (Exception e) {
            future.completeExceptionally(e);
          }
        } else {
          try {
            storage.close();
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

  /**
   * Raft protocol builder.
   */
  public static class Builder extends AbstractProtocol.Builder {
    private RaftStorage storage;
    private RaftConfig config = new RaftConfig();

    /**
     * Sets the Raft storage.
     *
     * @param storage The Raft storage.
     * @return The Raft protocol builder.
     */
    public Builder withStorage(RaftStorage storage) {
      this.storage = storage;
      return this;
    }

    /**
     * Sets the Raft election timeout, returning the Raft configuration for method chaining.
     *
     * @param electionTimeout The Raft election timeout in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the election timeout is not positive
     */
    public Builder withElectionTimeout(long electionTimeout) {
      config.setElectionTimeout(electionTimeout);
      return this;
    }

    /**
     * Sets the Raft election timeout, returning the Raft configuration for method chaining.
     *
     * @param electionTimeout The Raft election timeout.
     * @param unit The timeout unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the election timeout is not positive
     */
    public Builder withElectionTimeout(long electionTimeout, TimeUnit unit) {
      config.setElectionTimeout(electionTimeout, unit);
      return this;
    }

    /**
     * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
     *
     * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the heartbeat interval is not positive
     */
    public Builder withHeartbeatInterval(long heartbeatInterval) {
      config.setHeartbeatInterval(heartbeatInterval);
      return this;
    }

    /**
     * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
     *
     * @param heartbeatInterval The Raft heartbeat interval.
     * @param unit The heartbeat interval unit.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the heartbeat interval is not positive
     */
    public Builder withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
      config.setHeartbeatInterval(heartbeatInterval, unit);
      return this;
    }

    @Override
    public Protocol build() {
      return new RaftProtocol(storage, config);
    }
  }

}
