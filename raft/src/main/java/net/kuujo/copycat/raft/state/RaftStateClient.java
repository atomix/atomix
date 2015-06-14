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

import net.kuujo.copycat.cluster.ClusterException;
import net.kuujo.copycat.cluster.ManagedMembers;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.MemberInfo;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.rpc.*;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.ThreadChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Raft client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftStateClient implements Managed<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RaftStateClient.class);
  private static final long REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
  private final ManagedMembers members;
  private final ExecutionContext context;
  private final ThreadChecker threadChecker;
  private final AtomicBoolean keepAlive = new AtomicBoolean();
  private final Random random = new Random();
  private ScheduledFuture<?> currentTimer;
  private ScheduledFuture<?> registerTimer;
  private long keepAliveInterval = 1000;
  private volatile boolean open;
  private CompletableFuture<Void> openFuture;
  protected volatile int leader;
  protected volatile long term;
  protected volatile long session;
  private volatile long request;
  private volatile long response;
  private volatile long version;
  private final Map<Long, ScheduledFuture<?>> responseFutures = new HashMap<>();

  public RaftStateClient(ManagedMembers members, ExecutionContext context) {
    if (members == null)
      throw new NullPointerException("members cannot be null");
    this.members = members;
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
  RaftStateClient setLeader(int leader) {
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
  RaftStateClient setTerm(long term) {
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
  RaftStateClient setSession(long session) {
    this.session = session;
    this.request = 0;
    this.response = 0;
    this.version = 0;
    if (session != 0 && openFuture != null) {
      synchronized (openFuture) {
        if (openFuture != null) {
          CompletableFuture<Void> future = openFuture;
          context.execute(() -> {
            open = true;
            future.complete(null);
          });
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
  RaftStateClient setRequest(long request) {
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
  RaftStateClient setResponse(long response) {
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
  RaftStateClient setVersion(long version) {
    if (version > this.version)
      this.version = version;
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
  public RaftStateClient setKeepAliveInterval(long keepAliveInterval) {
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
      if (session == 0) {
        future.completeExceptionally(new IllegalStateException("session not open"));
        return;
      }

      long requestId = ++request;
      CommandRequest request = CommandRequest.builder()
        .withSession(getSession())
        .withRequest(requestId)
        .withResponse(getResponse())
        .withCommand(command)
        .build();

      this.<R>submit(request, future).whenComplete((result, error) -> {
        if (error == null) {
          future.complete(result);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return future;
  }

  /**
   * Recursively submits the command to the cluster.
   *
   * @param request The request to submit.
   * @return The completion future.
   */
  private <T> CompletableFuture<T> submit(CommandRequest request) {
    return submit(request, new CompletableFuture<>());
  }

  /**
   * Recursively submits the command to the cluster.
   *
   * @param request The request to submit.
   * @param future The future to complete once the command has succeeded.
   * @return The completion future.
   */
  private <T> CompletableFuture<T> submit(CommandRequest request, CompletableFuture<T> future) {
    Member member = selectMember(request.command());
    this.<T>submit(request, member).whenComplete((result, error) -> {
      if (error == null) {
        future.complete(result);
      } else if (error instanceof TimeoutException) {
        submit(request, future);
      } else if (error instanceof ClusterException) {
        LOGGER.warn("Failed to communicate with {}: {}", member, error);
        submit(request, future);
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Attempts to submit the request to the given member.
   *
   * @param request The request to submit.
   * @param member The member to which to submit the request.
   * @return A completable future to be completed with the result.
   */
  @SuppressWarnings("unchecked")
  private <T> CompletableFuture<T> submit(CommandRequest request, Member member) {
    CompletableFuture<T> future = new CompletableFuture<>();
    ScheduledFuture<?> timeoutFuture = context.schedule(() -> future.completeExceptionally(new TimeoutException("request timed out")), REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);

    LOGGER.debug("Submitting {} to {}", request, member);
    member.<CommandRequest, CommandResponse>send(request).whenComplete((response, error) -> {
      timeoutFuture.cancel(false);
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete((T) response.result());
        } else {
          future.completeExceptionally(response.error().createException());
        }
        setResponse(Math.max(getResponse(), request.request()));
      } else {
        future.completeExceptionally(error);
      }
      request.close();
    });

    return future;
  }

  /**
   * Selects the member to which to send the given command.
   */
  protected Member selectMember(Command<?> command) {
    int leader = getLeader();
    return leader == 0 ? members.members().get(random.nextInt(members.members().size())) : members.member(leader);
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

      this.<R>submit(request).whenComplete((result, error) -> {
        if (error == null) {
          future.complete(result);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return future;
  }

  /**
   * Recursively submits the command to the cluster.
   *
   * @param request The request to submit.
   * @return The completion future.
   */
  private <T> CompletableFuture<T> submit(QueryRequest request) {
    return submit(request, new CompletableFuture<>());
  }

  /**
   * Recursively submits the command to the cluster.
   *
   * @param request The request to submit.
   * @param future The future to complete once the command has succeeded.
   * @return The completion future.
   */
  private <T> CompletableFuture<T> submit(QueryRequest request, CompletableFuture<T> future) {
    this.<T>submit(request, selectMember(request.query())).whenComplete((result, error) -> {
      if (error == null) {
        future.complete(result);
      } else if (error instanceof TimeoutException) {
        submit(request, future);
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Attempts to submit the request to the given member.
   *
   * @param request The request to submit.
   * @param member The member to which to submit the request.
   * @return A completable future to be completed with the result.
   */
  @SuppressWarnings("unchecked")
  private <T> CompletableFuture<T> submit(QueryRequest request, Member member) {
    CompletableFuture<T> future = new CompletableFuture<>();
    ScheduledFuture<?> timeoutFuture = context.schedule(() -> future.completeExceptionally(new TimeoutException("request timed out")), REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);

    LOGGER.debug("Submitting {} to {}", request, member);
    member.<QueryRequest, QueryResponse>send(request).whenComplete((response, error) -> {
      timeoutFuture.cancel(false);
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete((T) response.result());
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
      }
      request.close();
    });

    return future;
  }

  /**
   * Selects the member to which to send the given query.
   */
  protected Member selectMember(Query<?> query) {
    ConsistencyLevel level = query.consistency();
    if (level.isLeaderRequired()) {
      int leader = getLeader();
      return leader == 0 ? members.members().get(random.nextInt(members.members().size())) : members.member(leader);
    } else {
      return members.members().get(random.nextInt(members.members().size()));
    }
  }

  /**
   * Registers the client.
   */
  private CompletableFuture<Void> register() {
    return register(100, new CompletableFuture<>());
  }

  /**
   * Registers the client.
   */
  private CompletableFuture<Void> register(long interval, CompletableFuture<Void> future) {
    register(new ArrayList<>(members.members())).whenCompleteAsync((result, error) -> {
      threadChecker.checkThread();
      if (error == null) {
        future.complete(null);
      } else {
        long nextInterval = Math.min(interval * 2, 5000);
        registerTimer = context.schedule(() -> register(nextInterval, future), nextInterval, TimeUnit.MILLISECONDS);
      }
    }, context);
    return future;
  }

  /**
   * Registers the client.
   */
  protected CompletableFuture<Void> register(List<Member> members) {
    return register(members, new CompletableFuture<>()).thenCompose(response -> {
      setTerm(response.term());
      setLeader(response.leader());
      setSession(response.session());
      return this.members.configure(response.members().toArray(new MemberInfo[response.members().size()]));
    });
  }

  /**
   * Registers the client by contacting a random member.
   */
  protected CompletableFuture<RegisterResponse> register(List<Member> members, CompletableFuture<RegisterResponse> future) {
    if (members.isEmpty()) {
      future.completeExceptionally(new NoLeaderException("no leader found"));
      return future;
    }

    Member member = selectMember(members);

    LOGGER.debug("{} - Registering session via {}", member.id(), member.id());
    RegisterRequest request = RegisterRequest.builder().build();
    member.<RegisterRequest, RegisterResponse>send(request).whenComplete((response, error) -> {
      threadChecker.checkThread();
      synchronized (openFuture) {
        if (openFuture != null) {
          if (error == null && response.status() == Response.Status.OK) {
            future.complete(response);
            LOGGER.debug("Registered new session: {}", getSession());
          } else {
            LOGGER.debug("Session registration failed, retrying");
            setLeader(0);
            register(members, future);
          }
        }
      }
    });
    return future;
  }

  /**
   * Starts the keep alive timer.
   */
  private void startKeepAliveTimer() {
    LOGGER.debug("Starting keep alive timer");
    currentTimer = context.scheduleAtFixedRate(this::keepAlive, 1, keepAliveInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Sends a keep alive request to a random member.
   */
  private void keepAlive() {
    if (keepAlive.compareAndSet(false, true)) {
      LOGGER.debug("Sending keep alive request");
      keepAlive(members.members().stream()
        .filter(m -> m.type() == Member.Type.ACTIVE)
        .collect(Collectors.toList())).thenRun(() -> keepAlive.set(false));
    }
  }

  /**
   * Sends a keep alive request.
   */
  protected CompletableFuture<Void> keepAlive(List<Member> members) {
    return keepAlive(members, new CompletableFuture<>()).thenCompose(response -> {
      setTerm(response.term());
      setLeader(response.leader());
      setVersion(response.version());
      return this.members.configure(response.members().toArray(new MemberInfo[response.members().size()]));
    });
  }

  /**
   * Registers the client by contacting a random member.
   */
  protected CompletableFuture<KeepAliveResponse> keepAlive(List<Member> members, CompletableFuture<KeepAliveResponse> future) {
    if (members.isEmpty()) {
      future.completeExceptionally(RaftError.Type.NO_LEADER_ERROR.createException());
      keepAlive.set(false);
      return future;
    }

    Member member = selectMember(members);

    KeepAliveRequest request = KeepAliveRequest.builder()
      .withSession(getSession())
      .build();
    member.<KeepAliveRequest, KeepAliveResponse>send(request).whenComplete((response, error) -> {
      threadChecker.checkThread();
      if (isOpen()) {
        if (error == null && response.status() == Response.Status.OK) {
          future.complete(response);
        } else {
          keepAlive(members, future);
        }
      }
    });
    return future;
  }

  /**
   * Selects a random member from the given members list.
   */
  protected Member selectMember(List<Member> members) {
    if (leader != 0) {
      for (int i = 0; i < members.size(); i++) {
        if (members.get(i).id() == leader) {
          return members.remove(i);
        }
      }
      setLeader(0);
      return members.remove(random.nextInt(members.size()));
    } else {
      return members.remove(random.nextInt(members.size()));
    }
  }

  /**
   * Cancels the register timer.
   */
  private void cancelRegisterTimer() {
    if (registerTimer != null) {
      LOGGER.debug("Cancelling register timer");
      registerTimer.cancel(false);
    }
  }

  /**
   * Cancels the keep alive timer.
   */
  private void cancelKeepAliveTimer() {
    if (currentTimer != null) {
      LOGGER.debug("Cancelling keep alive timer");
      currentTimer.cancel(false);
    }
  }

  @Override
  public CompletableFuture<Void> open() {
    openFuture = new CompletableFuture<>();
    members.open().thenRunAsync(() -> {
      register().thenRun(this::startKeepAliveTimer);
    }, context);
    return openFuture;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.execute(() -> {
      cancelRegisterTimer();
      cancelKeepAliveTimer();
      open = false;
      members.close().whenCompleteAsync((result, error) -> {
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

}
