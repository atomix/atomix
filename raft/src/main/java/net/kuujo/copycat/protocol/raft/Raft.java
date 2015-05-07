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

import net.kuujo.copycat.ConfigurationException;
import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBuffer;
import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.protocol.raft.rpc.Response;
import net.kuujo.copycat.protocol.raft.rpc.SubmitRequest;
import net.kuujo.copycat.protocol.raft.storage.RaftLog;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.ThreadChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Raft protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Raft implements ProtocolInstance {

  /**
   * Returns a new Raft builder.
   *
   * @return A new Raft builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final Logger LOGGER = LoggerFactory.getLogger(Raft.class);
  private final Buffer KEY = HeapBuffer.allocate(1024, 1024 * 1024);
  private final Buffer ENTRY = HeapBuffer.allocate(1024, 1024 * 1024);
  private final Set<EventListener<Member>> electionListeners = new CopyOnWriteArraySet<>();
  private final RaftConfig config;
  private final RaftLog log;
  private final Cluster cluster;
  private final String topic;
  private final ExecutionContext context;
  private final ThreadChecker threadChecker;
  private ProtocolHandler handler;
  private RaftState state;
  private final Map<Integer, RaftMember> members = new HashMap<>();
  private CompletableFuture<ProtocolInstance> openFuture;
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

  protected Raft(RaftLog log, RaftConfig config, Cluster cluster, String topic, ExecutionContext context) {
    this.log = log;
    this.config = config;
    this.cluster = cluster;
    this.topic = topic;
    this.context = context;
    this.threadChecker = new ThreadChecker(context);
  }

  /**
   * Returns the Raft cluster.
   *
   * @return The Raft cluster.
   */
  public Cluster cluster() {
    return cluster;
  }

  /**
   * Returns the Raft topic.
   *
   * @return The Raft cluster topic.
   */
  public String topic() {
    return topic;
  }

  /**
   * Returns the execution context.
   *
   * @return The Raft execution context.
   */
  public ExecutionContext context() {
    return context;
  }

  /**
   * Adds an election listener.
   *
   * @param listener The election listener.
   * @return The Raft protocol.
   */
  public Raft addElectionListener(EventListener<Member> listener) {
    electionListeners.add(listener);
    return this;
  }

  /**
   * Removes an election listener.
   *
   * @param listener The election listener.
   * @return The Raft protocol.
   */
  public Raft removeElectionListener(EventListener<Member> listener) {
    electionListeners.remove(listener);
    return this;
  }

  @Override
  public Raft setFilter(ProtocolFilter filter) {
    log.filter(entry -> {
      entry.readKey(KEY);
      entry.readEntry(ENTRY);
      return filter.accept(entry.index(), KEY.flip(), ENTRY.flip());
    });
    return this;
  }

  @Override
  public Raft setHandler(ProtocolHandler handler) {
    this.handler = handler;
    return this;
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
  Raft updateMembers(Collection<RaftMember> members) {
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
  Raft setLeader(int leader) {
    if (this.leader == 0) {
      if (leader != 0) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", cluster.member().id(), leader);
        if (openFuture != null) {
          openFuture.complete(this);
          openFuture = null;
        }
        electionListeners.forEach(l -> l.accept(cluster.member(this.leader)));
      }
    } else if (leader != 0) {
      if (this.leader != leader) {
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.debug("{} - Found leader {}", cluster.member().id(), leader);
        electionListeners.forEach(l -> l.accept(cluster.member(this.leader)));
      }
    } else {
      this.leader = 0;
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
  Raft setTerm(long term) {
    if (term > this.term) {
      long oldTerm = this.term;
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
  long getTerm() {
    return term;
  }

  /**
   * Sets the state version.
   *
   * @param version The state version.
   * @return The Raft context.
   */
  Raft setVersion(long version) {
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
  Raft setLastVotedFor(int candidate) {
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
  Raft setCommitIndex(long commitIndex) {
    if (commitIndex < 0)
      throw new IllegalArgumentException("commit index must be positive");
    if (commitIndex < this.commitIndex)
      throw new IllegalArgumentException("cannot decrease commit index");
    if (firstCommitIndex == 0) {
      if (commitIndex == 0) {
        if (recovering) {
          recovering = false;
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
  Raft setRecycleIndex(long recycleIndex) {
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
  Raft setLastApplied(long lastApplied) {
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

  /**
   * Commits an entry to the context.
   *
   * @param key The entry key.
   * @param entry The entry value.
   * @param result The buffer to which to write the commit result.
   * @return The result buffer.
   */
  Buffer commit(long index, Buffer key, Buffer entry, Buffer result) {
    if (handler != null) {
      return handler.apply(index, key, entry, result);
    }
    return result;
  }

  /**
   * Returns the state log.
   *
   * @return The state log.
   */
  RaftLog log() {
    return log;
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
        this.state = state.type().getConstructor(Raft.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    } else {
      // Force state transitions to occur synchronously in order to prevent race conditions.
      try {
        this.state = state.type().getConstructor(Raft.class).newInstance(this);
        this.state.open().get();
      } catch (InterruptedException | ExecutionException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException("failed to initialize Raft state", e);
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public synchronized CompletableFuture<ProtocolInstance> open() {
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
            log.open();
            transition(RaftState.Type.PASSIVE);
            break;
          case ACTIVE:
            log.open();
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
    context.execute(() -> {
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

  /**
   * Raft builder.
   */
  public static class Builder implements net.kuujo.copycat.Builder<Raft> {
    private RaftLog log;
    private RaftConfig config = new RaftConfig();
    private Cluster cluster;
    private String topic;
    private ExecutionContext context;

    /**
     * Sets the Raft log.
     *
     * @param log The Raft log.
     * @return The Raft builder.
     */
    public Builder withLog(RaftLog log) {
      this.log = log;
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

    /**
     * Sets the Raft cluster.
     *
     * @param cluster The Raft cluster.
     * @return The Raft builder.
     */
    public Builder withCluster(Cluster cluster) {
      this.cluster = cluster;
      return this;
    }

    /**
     * Sets the Raft cluster topic.
     *
     * @param topic The Raft cluster topic.
     * @return The Raft builder.
     */
    public Builder withTopic(String topic) {
      this.topic = topic;
      return this;
    }

    /**
     * Sets the Raft execution context.
     *
     * @param context The execution context.
     * @return The Raft builder.
     */
    public Builder withContext(ExecutionContext context) {
      this.context = context;
      return this;
    }

    @Override
    public Raft build() {
      if (log == null)
        throw new ConfigurationException("log not configured");
      if (cluster == null)
        throw new ConfigurationException("cluster not configured");
      if (topic == null)
        throw new ConfigurationException("topic not configured");
      return new Raft(log, config, cluster, topic, context != null ? context : new ExecutionContext("copycat-" + topic));
    }
  }

}
