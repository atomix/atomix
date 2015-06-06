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
import net.kuujo.copycat.cluster.MemberInfo;
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.NoLeaderException;
import net.kuujo.copycat.raft.Query;
import net.kuujo.copycat.raft.RaftError;
import net.kuujo.copycat.raft.rpc.*;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.ThreadChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Raft client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftClient implements Managed<RaftClient> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RaftClient.class);
  private final ManagedCluster cluster;
  private final ExecutionContext context;
  private final ThreadChecker threadChecker;
  private final AtomicBoolean keepAlive = new AtomicBoolean();
  private final Random random = new Random();
  private ScheduledFuture<?> currentTimer;
  private ScheduledFuture<?> registerTimer;
  private long reconnectInterval = 1000;
  private long keepAliveInterval = 1000;
  private boolean open;
  private CompletableFuture<RaftClient> openFuture;
  private int leader;
  private long term;
  private long session;
  private long request;
  private long response;
  private long version;

  public RaftClient(ManagedCluster cluster, ExecutionContext context) {
    if (cluster == null)
      throw new NullPointerException("cluster cannot be null");
    if (context == null)
      throw new NullPointerException("context cannot be null");
    this.cluster = cluster;
    this.context = context;
    this.threadChecker = new ThreadChecker(context);
  }

  /**
   * Returns the cluster leader.
   *
   * @return The cluster leader.
   */
  public int getLeader() {
    return leader;
  }

  /**
   * Sets the cluster leader.
   *
   * @param leader The cluster leader.
   * @return The Raft client.
   */
  RaftClient setLeader(int leader) {
    this.leader = leader;
    return this;
  }

  /**
   * Returns the cluster term.
   *
   * @return The cluster term.
   */
  public long getTerm() {
    return term;
  }

  /**
   * Sets the cluster term.
   *
   * @param term The cluster term.
   * @return The Raft client.
   */
  RaftClient setTerm(long term) {
    this.term = term;
    return this;
  }

  /**
   * Returns the client session.
   *
   * @return The client session.
   */
  public long getSession() {
    return session;
  }

  /**
   * Sets the client session.
   *
   * @param session The client session.
   * @return The Raft client.
   */
  private RaftClient setSession(long session) {
    this.session = session;
    this.request = 0;
    this.response = 0;
    this.version = 0;
    if (session != 0 && openFuture != null) {
      synchronized (openFuture) {
        if (openFuture != null) {
          CompletableFuture<RaftClient> future = openFuture;
          context.execute(() -> future.complete(this));
          openFuture = null;
        }
      }
    }
    return this;
  }

  /**
   * Returns the client request number.
   *
   * @return The client request number.
   */
  public long getRequest() {
    return request;
  }

  /**
   * Sets the client request number.
   *
   * @param request The client request number.
   * @return The Raft client.
   */
  RaftClient setRequest(long request) {
    this.request = request;
    return this;
  }

  /**
   * Returns the client response number.
   *
   * @return The client response number.
   */
  public long getResponse() {
    return response;
  }

  /**
   * Sets the client response number.
   *
   * @param response The client response number.
   * @return The Raft client.
   */
  RaftClient setResponse(long response) {
    this.response = response;
    return this;
  }

  /**
   * Returns the client version.
   *
   * @return The client version.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Sets the client version.
   *
   * @param version The client version.
   * @return The Raft client.
   */
  RaftClient setVersion(long version) {
    if (version > this.version)
      this.version = version;
    return this;
  }

  /**
   * Returns the reconnect interval.
   *
   * @return The reconnect interval.
   */
  public long getReconnectInterval() {
    return reconnectInterval;
  }

  /**
   * Sets the reconnect interval.
   *
   * @param reconnectInterval The reconnect interval.
   * @return The Raft client.
   */
  RaftClient setReconnectInterval(long reconnectInterval) {
    if (reconnectInterval <= 0)
      throw new IllegalArgumentException("reconnect interval must be positive");
    this.reconnectInterval = reconnectInterval;
    return this;
  }

  /**
   * Returns the keep alive interval.
   *
   * @return The keep alive interval.
   */
  public long getKeepAliveInterval() {
    return keepAliveInterval;
  }

  /**
   * Sets the keep alive interval.
   *
   * @param keepAliveInterval The keep alive interval.
   * @return The Raft client.
   */
  RaftClient setKeepAliveInterval(long keepAliveInterval) {
    if (keepAliveInterval <= 0)
      throw new IllegalArgumentException("keep alive interval must be positive");
    this.keepAliveInterval = keepAliveInterval;
    return this;
  }

  /**
   * Submits a command.
   *
   * @param command The command to submit.
   * @param <R> The command result type.
   * @return A completable future to be completed with the command result.
   */
  @SuppressWarnings("unchecked")
  public <R> CompletableFuture<R> submit(Command<R> command) {
    if (!open)
      throw new IllegalStateException("protocol not open");

    CompletableFuture<R> future = new CompletableFuture<>();
    context.execute(() -> {
      if (leader == 0)
        future.completeExceptionally(new IllegalStateException("unknown leader"));
      if (session == 0)
        future.completeExceptionally(new IllegalStateException("session not open"));

      // TODO: This should retry on timeouts with the same request ID.
      long requestId = request++;
      CommandRequest request = CommandRequest.builder()
        .withSession(getSession())
        .withRequest(requestId)
        .withResponse(getResponse())
        .withCommand(command)
        .build();
      cluster.member(leader).<CommandRequest, CommandResponse>send(request).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status() == Response.Status.OK) {
            future.complete((R) response.result());
          } else {
            future.completeExceptionally(response.error().createException());
          }
          setResponse(Math.max(getResponse(), requestId));
        } else {
          future.completeExceptionally(error);
        }
        request.close();
      });
    });
    return future;
  }

  /**
   * Submits a query.
   *
   * @param query The query to submit.
   * @param <R> The query result type.
   * @return A completable future to be completed with the query result.
   */
  @SuppressWarnings("unchecked")
  public <R> CompletableFuture<R> submit(Query<R> query) {
    if (!open)
      throw new IllegalStateException("protocol not open");

    CompletableFuture<R> future = new CompletableFuture<>();
    context.execute(() -> {
      if (leader == 0)
        future.completeExceptionally(new IllegalStateException("unknown leader"));
      if (session == 0)
        future.completeExceptionally(new IllegalStateException("session not open"));

      QueryRequest request = QueryRequest.builder()
        .withSession(getSession())
        .withQuery(query)
        .build();
      cluster.member(leader).<QueryRequest, QueryResponse>send(request).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status() == Response.Status.OK) {
            future.complete((R) response.result());
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
   * Registers the client.
   */
  private CompletableFuture<Void> register() {
    return register(new CompletableFuture<>());
  }

  /**
   * Registers the client.
   */
  private CompletableFuture<Void> register(CompletableFuture<Void> future) {
    register(cluster.members().stream().filter(m -> m.type() == Member.Type.ACTIVE)
      .collect(Collectors.toList()), new CompletableFuture<>()).whenComplete((result, error) -> {
      if (error == null) {
        future.complete(null);
      } else {
        registerTimer = context.schedule(() -> register(future), reconnectInterval, TimeUnit.MILLISECONDS);
      }
    });
    return future;
  }

  /**
   * Registers the client by contacting a random member.
   */
  private CompletableFuture<Long> register(List<Member> members, CompletableFuture<Long> future) {
    if (members.isEmpty()) {
      future.completeExceptionally(new NoLeaderException("no leader found"));
      return future;
    }

    Member member;
    if (leader != 0) {
      member = cluster.member(leader);
    } else {
      member = members.remove(random.nextInt(members.size()));
    }

    LOGGER.debug("{} - Registering session via {}", cluster.member().id(), member.id());
    RegisterRequest request = RegisterRequest.builder()
      .withMember(cluster.member().info())
      .build();
    member.<RegisterRequest, RegisterResponse>send(request).whenComplete((response, error) -> {
      threadChecker.checkThread();
      if (openFuture != null) {
        if (error == null && response.status() == Response.Status.OK) {
          setTerm(response.term());
          setLeader(response.leader());
          setSession(response.session());
          cluster.configure(response.members().toArray(new MemberInfo[response.members().size()])).whenComplete((configureResult, configureError) -> {
            if (configureError == null) {
              future.complete(response.session());
            } else {
              future.completeExceptionally(configureError);
            }
          });
          LOGGER.debug("{} - Registered new session: {}", cluster.member().id(), getSession());
        } else {
          LOGGER.debug("{} - Session registration failed, retrying", cluster.member().id());
          setLeader(0);
          register(members, future);
        }
      }
    });
    return future;
  }

  /**
   * Starts the keep alive timer.
   */
  private void startKeepAliveTimer() {
    LOGGER.debug("{} - Starting keep alive timer", cluster.member().id());
    currentTimer = context.scheduleAtFixedRate(this::keepAlive, 1, keepAliveInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Sends a keep alive request to a random member.
   */
  private void keepAlive() {
    if (keepAlive.compareAndSet(false, true)) {
      LOGGER.debug("{} - Sending keep alive request", cluster.member().id());
      keepAlive(cluster.members().stream()
        .filter(m -> m.type() == Member.Type.ACTIVE)
        .collect(Collectors.toList()), new CompletableFuture<>());
    }
  }

  /**
   * Registers the client by contacting a random member.
   */
  private CompletableFuture<Void> keepAlive(List<Member> members, CompletableFuture<Void> future) {
    if (members.isEmpty()) {
      future.completeExceptionally(RaftError.Type.NO_LEADER_ERROR.createException());
      keepAlive.set(false);
      return future;
    }

    Member member;
    if (leader != 0) {
      member = cluster.member(leader);
    } else {
      member = members.remove(random.nextInt(members.size()));
    }

    KeepAliveRequest request = KeepAliveRequest.builder()
      .withSession(getSession())
      .build();
    member.<KeepAliveRequest, KeepAliveResponse>send(request).whenComplete((response, error) -> {
      threadChecker.checkThread();
      if (isOpen()) {
        if (error == null && response.status() == Response.Status.OK) {
          setTerm(response.term());
          setLeader(response.leader());
          setVersion(response.version());
          cluster.configure(response.members().toArray(new MemberInfo[response.members().size()])).whenComplete((configureResult, configureError) -> {
            if (configureError == null) {
              future.complete(null);
            } else {
              future.completeExceptionally(configureError);
            }
            keepAlive.set(false);
          });
        } else {
          keepAlive(members, future);
        }
      }
    });
    return future;
  }

  /**
   * Cancels the register timer.
   */
  private void cancelRegisterTimer() {
    if (registerTimer != null) {
      LOGGER.debug("{} - cancelling register timer", cluster.member().id());
      registerTimer.cancel(false);
    }
  }

  /**
   * Cancels the keep alive timer.
   */
  private void cancelKeepAliveTimer() {
    if (currentTimer != null) {
      LOGGER.debug("{} - cancelling keep alive timer", cluster.member().id());
      currentTimer.cancel(false);
    }
  }

  @Override
  public CompletableFuture<RaftClient> open() {
    openFuture = new CompletableFuture<>();
    context.execute(() -> {
      register().thenRun(this::startKeepAliveTimer).thenApply(v -> {
        open = true;
        return this;
      });
    });
    return openFuture;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.runAsync(() -> {
      cancelRegisterTimer();
      cancelKeepAliveTimer();
      open = false;
    }, context);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
