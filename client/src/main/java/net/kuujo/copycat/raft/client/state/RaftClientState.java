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
package net.kuujo.copycat.raft.client.state;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.transport.Client;
import net.kuujo.copycat.transport.Connection;
import net.kuujo.copycat.transport.Transport;
import net.kuujo.copycat.transport.TransportException;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.Futures;
import net.kuujo.copycat.util.concurrent.SingleThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Raft client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftClientState implements Managed<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RaftClientState.class);
  private static final long REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
  private final int id;
  private final Members members;
  private final Transport transport;
  private final Client client;
  private Member member;
  private Connection connection;
  private final Context context;
  private CompletableFuture<Void> registerFuture;
  private final AtomicBoolean keepAlive = new AtomicBoolean();
  private final Random random = new Random();
  private ScheduledFuture<?> keepAliveTimer;
  private ScheduledFuture<?> registerTimer;
  private long keepAliveInterval = 1000;
  private volatile boolean open;
  private CompletableFuture<Void> openFuture;
  protected volatile int leader;
  protected volatile long term;
  private volatile ClientSession session;
  protected volatile long sessionId;
  private volatile long request;
  private volatile long response;
  private volatile long version;

  public RaftClientState(Transport transport, Members members, Alleycat serializer) {
    this(0, transport, members, serializer);
  }

  protected RaftClientState(int clientId, Transport transport, Members members, Alleycat serializer) {
    if (members == null)
      throw new NullPointerException("members cannot be null");

    this.id = clientId;
    this.context = new SingleThreadContext("copycat-client-" + clientId, serializer.clone());
    this.members = members;
    this.transport = transport;
    this.client = transport.client(UUID.randomUUID());
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
  RaftClientState setLeader(int leader) {
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
  RaftClientState setTerm(long term) {
    this.term = term;
    return this;
  }

  /**
   * Returns the client session.
   *
   * @return The client session.
   */
  public Session getSession() {
    return session;
  }

  /**
   * Returns the client session.
   *
   * @return The client session.
   */
  public long getSessionId() {
    return sessionId;
  }

  /**
   * Sets the client session.
   *
   * @param sessionId The client session.
   * @return The Raft client.
   */
  protected RaftClientState setSessionId(long sessionId) {
    this.sessionId = sessionId;
    this.request = 0;
    this.response = 0;
    this.version = 0;
    if (sessionId != 0) {

    } else if (session != null) {
      session.close();
    }
    if (sessionId != 0 && openFuture != null) {
      synchronized (this) {
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
  protected RaftClientState setRequest(long request) {
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
  protected RaftClientState setResponse(long response) {
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
  protected RaftClientState setVersion(long version) {
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
  public RaftClientState setKeepAliveInterval(long keepAliveInterval) {
    if (keepAliveInterval <= 0)
      throw new IllegalArgumentException("keep alive interval must be positive");
    this.keepAliveInterval = keepAliveInterval;
    return this;
  }

  /**
   * Gets a connection to a specific member.
   *
   * @param member The member for which to get the connection.
   * @return A completable future to be completed once the connection has been connected.
   */
  protected CompletableFuture<Connection> getConnection(Member member) {
    if (connection != null && member.equals(this.member)) {
      return CompletableFuture.completedFuture(connection);
    }

    final InetSocketAddress address;
    try {
      address = new InetSocketAddress(InetAddress.getByName(member.host()), member.port());
    } catch (UnknownHostException e) {
      return Futures.exceptionalFuture(e);
    }

    Function<Connection, Connection> connectHandler = connection -> {
      this.connection = connection;
      this.member = member;
      if (session != null) {
        session.setConnection(connection);
      }
      connection.closeListener(c -> this.connection = null);
      connection.exceptionListener(e -> this.connection = null);
      return connection;
    };

    if (connection != null) {
      return connection.close().thenCompose(v -> client.connect(address)).thenApply(connectHandler);
    }

    return client.connect(address).thenApply(connectHandler);
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
      long requestId = ++request;
      CommandRequest request = CommandRequest.builder()
        .withSession(getSessionId())
        .withRequest(requestId)
        .withResponse(getResponse())
        .withCommand(command)
        .build();

      if (sessionId == 0) {
        register().thenRun(() -> this.<R>submit(request).whenComplete((result, error) -> {
          if (error == null) {
            future.complete(result);
          } else {
            future.completeExceptionally(error);
          }
        }));
      } else {
        this.<R>submit(request).whenComplete((result, error) -> {
          if (error == null) {
            future.complete(result);
          } else {
            future.completeExceptionally(error);
          }
        });
      }
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
        request.close();
        future.complete(result);
      } else if (error instanceof TimeoutException) {
        submit(request, future);
      } else if (error instanceof NoLeaderException) {
        submit(request, future);
      } else if (error instanceof TransportException) {
        LOGGER.warn("Failed to communicate with {}: {}", member, error);
        submit(request, future);
      } else if (error instanceof UnknownSessionException) {
        LOGGER.warn("Lost session: {}", getSessionId());

        setSessionId(0);
        if (session != null) {
          session.expire();
          session = null;
        }

        register().thenRun(() -> {
          submit(CommandRequest.builder(request)
            .withSession(getSessionId())
            .build(), future);
        });
      } else {
        request.close();
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
    getConnection(member).thenAccept(connection -> {
      connection.<CommandRequest, CommandResponse>send(request).whenComplete((response, error) -> {
        timeoutFuture.cancel(false);
        if (error == null) {
          if (response.status() == Response.Status.OK) {
            future.complete((T) response.result());
          } else {
            future.completeExceptionally(response.error().instance());
          }
          setResponse(Math.max(getResponse(), request.request()));
        } else {
          future.completeExceptionally(error);
        }
      });
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
      QueryRequest request = QueryRequest.builder()
        .withSession(getSessionId())
        .withQuery(query)
        .build();

      if (sessionId == 0) {
        register().thenRun(() -> this.<R>submit(request).whenComplete((result, error) -> {
          if (error == null) {
            future.complete(result);
          } else {
            future.completeExceptionally(error);
          }
        }));
      } else {
        this.<R>submit(request).whenComplete((result, error) -> {
          if (error == null) {
            future.complete(result);
          } else {
            future.completeExceptionally(error);
          }
        });
      }
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
    Member member = selectMember(request.query());
    this.<T>submit(request, member).whenComplete((result, error) -> {
      if (error == null) {
        request.close();
        future.complete(result);
      } else if (error instanceof TimeoutException) {
        submit(request, future);
      } else if (error instanceof NoLeaderException) {
        submit(request, future);
      } else if (error instanceof TransportException) {
        LOGGER.warn("Failed to communicate with {}: {}", member, error);
        submit(request, future);
      } else if (error instanceof UnknownSessionException) {
        LOGGER.warn("Lost session: {}", getSessionId());
        setSessionId(0);
        if (session != null) {
          session.expire();
          session = null;
        }

        register().thenRun(() -> {
          submit(QueryRequest.builder(request)
            .withSession(getSessionId())
            .build(), future);
        });
      } else {
        request.close();
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
    getConnection(member).thenAccept(connection -> {
      connection.<QueryRequest, QueryResponse>send(request).whenComplete((response, error) -> {
        timeoutFuture.cancel(false);
        if (error == null) {
          if (response.status() == Response.Status.OK) {
            future.complete((T) response.result());
          } else {
            future.completeExceptionally(response.error().instance());
          }
        } else {
          future.completeExceptionally(error);
        }
      });
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
    context.checkThread();
    if (registerFuture == null) {
      registerFuture = register(100, new CompletableFuture<>()).whenComplete((result, error) -> {
        registerFuture = null;
      });
    }
    return registerFuture;
  }

  /**
   * Registers the client.
   */
  private CompletableFuture<Void> register(long interval, CompletableFuture<Void> future) {
    register(new ArrayList<>(members.members())).whenCompleteAsync((result, error) -> {
      context.checkThread();
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
    return register(members, new CompletableFuture<>()).thenAccept(response -> {
      setTerm(response.term());
      setLeader(response.leader());
      setSessionId(response.session());
      session = new ClientSession(response.session(), id, client.id(), context);
      this.members.configure(response.members());
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

    RegisterRequest request = RegisterRequest.builder()
      .withMember(id)
      .withConnection(client.id())
      .build();

    LOGGER.debug("Sending {} to {}", request, member);
    getConnection(member).thenAccept(connection -> {
      context.checkThread();
      connection.<RegisterRequest, RegisterResponse>send(request).whenComplete((response, error) -> {
        context.checkThread();
        if (error == null && response.status() == Response.Status.OK) {
          future.complete(response);
          LOGGER.debug("Registered new session: {}", getSessionId());
        } else {
          LOGGER.debug("Session registration failed, retrying");
          setLeader(0);
          register(members, future);
        }
      });
    });
    return future;
  }

  /**
   * Starts the keep alive timer.
   */
  private void startKeepAliveTimer() {
    LOGGER.debug("Starting keep alive timer");
    keepAliveTimer = context.scheduleAtFixedRate(this::keepAlive, 1, keepAliveInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Sends a keep alive request to a random member.
   */
  private void keepAlive() {
    if (keepAlive.compareAndSet(false, true) && getSessionId() != 0) {
      keepAlive(members.members().stream()
        .filter(m -> m.type() == Member.Type.ACTIVE)
        .collect(Collectors.toList())).whenComplete((result, error) -> keepAlive.set(false));
    }
  }

  /**
   * Sends a keep alive request.
   */
  protected CompletableFuture<Void> keepAlive(List<Member> members) {
    return keepAlive(members, new CompletableFuture<>()).thenAccept(response -> {
      setTerm(response.term());
      setLeader(response.leader());
      setVersion(response.version());
      this.members.configure(response.members());
    });
  }

  /**
   * Registers the client by contacting a random member.
   */
  protected CompletableFuture<KeepAliveResponse> keepAlive(List<Member> members, CompletableFuture<KeepAliveResponse> future) {
    if (members.isEmpty()) {
      future.completeExceptionally(RaftError.Type.NO_LEADER_ERROR.instance());
      keepAlive.set(false);
      return future;
    }

    Member member = selectMember(members);

    KeepAliveRequest request = KeepAliveRequest.builder()
      .withSession(getSessionId())
      .build();
    LOGGER.debug("Sending {} to {}", request, member);
    getConnection(member).thenAccept(connection -> {
      context.checkThread();
      if (isOpen()) {
        connection.<KeepAliveRequest, KeepAliveResponse>send(request).whenComplete((response, error) -> {
          if (isOpen()) {
            if (error == null && response.status() == Response.Status.OK) {
              future.complete(response);
            } else {
              future.completeExceptionally(error);
            }
          }
        });
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
    registerFuture = null;
  }

  /**
   * Cancels the keep alive timer.
   */
  private void cancelKeepAliveTimer() {
    if (keepAliveTimer != null) {
      LOGGER.debug("Cancelling keep alive timer");
      keepAliveTimer.cancel(false);
    }
  }

  @Override
  public CompletableFuture<Void> open() {
    openFuture = new CompletableFuture<>();
    register().thenRun(this::startKeepAliveTimer);
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
      if (session != null) {
        session.close();
        session = null;
      }

      open = false;
      transport.close().whenCompleteAsync((result, error) -> {
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
