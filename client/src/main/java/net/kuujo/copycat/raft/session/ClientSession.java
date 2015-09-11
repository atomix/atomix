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
package net.kuujo.copycat.raft.session;

import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.transport.Address;
import net.kuujo.copycat.io.transport.Client;
import net.kuujo.copycat.io.transport.Connection;
import net.kuujo.copycat.io.transport.Transport;
import net.kuujo.copycat.raft.protocol.Command;
import net.kuujo.copycat.raft.protocol.Query;
import net.kuujo.copycat.raft.protocol.error.RaftError;
import net.kuujo.copycat.raft.protocol.error.UnknownSessionException;
import net.kuujo.copycat.raft.protocol.request.*;
import net.kuujo.copycat.raft.protocol.response.*;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.Listener;
import net.kuujo.copycat.util.Listeners;
import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.util.concurrent.Context;
import net.kuujo.copycat.util.concurrent.Futures;
import net.kuujo.copycat.util.concurrent.Scheduled;
import net.kuujo.copycat.util.concurrent.SingleThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Client session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClientSession implements Session, Managed<Session> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientSession.class);

  /**
   * Client session state.
   */
  private enum State {
    OPEN,
    CLOSED,
    EXPIRED
  }

  private final Random random = new Random();
  private final Client client;
  private Set<Address> members;
  private final Duration keepAliveInterval;
  private final Context context;
  private List<Address> connectMembers;
  private Connection connection;
  private volatile State state = State.CLOSED;
  private volatile long id;
  private long timeout;
  private long failureTime;
  private CompletableFuture<Connection> connectFuture;
  private Scheduled retryFuture;
  private final List<Runnable> retries = new ArrayList<>();
  private Scheduled keepAliveFuture;
  private final Listeners<Session> openListeners = new Listeners<>();
  private final Listeners<Object> receiveListeners = new Listeners<>();
  private final Listeners<Session> closeListeners = new Listeners<>();
  private long requestSequence;
  private long responseSequence;
  private long eventVersion;
  private long eventSequence;
  private long version;

  public ClientSession(Transport transport, Collection<Address> members, Duration keepAliveInterval, Serializer serializer) {
    UUID id = UUID.randomUUID();
    this.client = Assert.notNull(transport, "transport").client(id);
    this.members = new HashSet<>(Assert.notNull(members, "members"));
    this.keepAliveInterval = keepAliveInterval;
    this.context = new SingleThreadContext("copycat-client-" + id.toString(), Assert.notNull(serializer, "serializer").clone());
    this.connectMembers = new ArrayList<>(members);
  }

  @Override
  public long id() {
    return id;
  }

  /**
   * Returns the session context.
   *
   * @return The session context.
   */
  public Context context() {
    return context;
  }

  /**
   * Sets the client remote members.
   */
  private void setMembers(Collection<Address> members) {
    this.members = new HashSet<>(members);
    this.connectMembers = new ArrayList<>(this.members);
  }

  /**
   * Sets the session timeout.
   */
  private void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  /**
   * Submits a command via the session.
   *
   * @param command The command to submit.
   * @param <T> The command output type.
   * @return A completable future to be completed with the command output.
   */
  public <T> CompletableFuture<T> submit(Command<T> command) {
    if (!isOpen())
      return Futures.exceptionalFuture(new IllegalStateException("session not open"));

    CompletableFuture<T> future = new CompletableFuture<>();
    context.executor().execute(() -> {

      CommandRequest request = CommandRequest.builder()
        .withSession(id)
        .withSequence(++requestSequence)
        .withCommand(command)
        .build();

      submit(request, future);
    });
    return future;
  }

  /**
   * Recursively submits a command.
   */
  @SuppressWarnings("unchecked")
  private <T> CompletableFuture<T> submit(CommandRequest request, CompletableFuture<T> future) {
    if (!isOpen()) {
      future.completeExceptionally(new IllegalStateException("session not open"));
      return future;
    }

    request.acquire();
    this.<CommandRequest, CommandResponse>request(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          responseSequence = Math.max(responseSequence, request.sequence());
          version = Math.max(version, response.version());
          future.complete((T) response.result());
          resetMembers();
        } else {
          future.completeExceptionally(response.error().createException());
        }
        response.release();
      } else {
        future.completeExceptionally(error);
      }
      request.release();
    });
    return future;
  }

  /**
   * Submits a query via the session.
   *
   * @param query The query to submit.
   * @param <T> The query output type.
   * @return A completable future to be completed with the query output.
   */
  public <T> CompletableFuture<T> submit(Query<T> query) {
    if (!isOpen())
      return Futures.exceptionalFuture(new IllegalStateException("session not open"));

    CompletableFuture<T> future = new CompletableFuture<>();
    context.executor().execute(() -> {

      QueryRequest request = QueryRequest.builder()
        .withSession(id)
        .withVersion(responseSequence)
        .withQuery(query)
        .build();

      submit(request, future);
    });
    return future;
  }

  /**
   * Recursively submits a query.
   */
  @SuppressWarnings("unchecked")
  private <T> CompletableFuture<T> submit(QueryRequest request, CompletableFuture<T> future) {
    if (!isOpen()) {
      future.completeExceptionally(new IllegalStateException("session not open"));
      return future;
    }

    request.acquire();
    this.<QueryRequest, QueryResponse>request(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          version = Math.max(version, response.version());
          future.complete((T) response.result());
          resetMembers();
        } else {
          future.completeExceptionally(response.error().createException());
        }
        response.release();
      } else {
        future.completeExceptionally(error);
      }
      request.release();
    });
    return future;
  }

  /**
   * Sends a session request.
   *
   * @param request The request to send.
   * @param <T> The request type.
   * @param <U> The response type.
   * @return A completable future to be completed once the response is received.
   */
  private <T extends SessionRequest<T>, U extends SessionResponse<U>> CompletableFuture<U> request(T request) {
    if (!isOpen())
      return Futures.exceptionalFutureAsync(new IllegalStateException("session not open"), context.executor());
    return request(request, new CompletableFuture<U>(), true, true);
  }

  /**
   * Sends a session request.
   *
   * @param request The request to send.
   * @param future The future to complete once the response is received.
   * @param checkOpen Whether to check if the session is open.
   * @param <T> The request type.
   * @param <U> The response type.
   * @return The provided future to be completed once the response is received.
   */
  private <T extends Request<T>, U extends Response<U>> CompletableFuture<U> request(T request, CompletableFuture<U> future, boolean checkOpen, boolean recordFailures) {
    context.checkThread();

    // If the session already expired, immediately fail the future.
    if (checkOpen && !isOpen()) {
      future.completeExceptionally(new IllegalStateException("session expired"));
      return future;
    }

    // If we're already connected to a server, use the existing connection. The connection will be reset in the event
    // of an error on any connection, or callers can reset connections as well.
    if (connection != null) {
      return request(request, connection, future, checkOpen, true);
    }

    // If we've run out of servers to which to attempt to connect, determine whether we should expire the
    // session based on the responses from servers with which we did successfully communicate and the
    // time we were last able to successfully communicate with a correct server process. The failureTime
    // indicates the first time we received a NO_LEADER_ERROR from a server.
    if (connectMembers.isEmpty()) {
      // If open checks are not being performed, don't retry connecting to the servers. Simply fail.
      if (!checkOpen) {
        LOGGER.warn("Failed to connect to cluster");
        future.completeExceptionally(new IllegalStateException("session not open"));
      }
      // If retries have already been scheduled, queue a callback to be called to retry the request.
      else if (retryFuture != null) {
        retries.add(() -> {
          LOGGER.debug("Retrying: {}", request);
          request(request, future, true, true);
        });
      }
      // If all servers indicated that no leader could be found and a session timeout has elapsed, time out the session.
      else if (failureTime > 0 && failureTime + timeout < System.currentTimeMillis()) {
        LOGGER.warn("Lost session");
        resetConnection().onExpire();
        future.completeExceptionally(new IllegalStateException("session expired"));
      }
      // If not all servers responded with a NO_LEADER_EXCEPTION or less than a session timeout has expired,
      // schedule a retry to attempt to connect to servers again.
      else {
        LOGGER.warn("Failed to communicate with cluster. Retrying");
        retryFuture = context.schedule(this::retryRequests, Duration.ofMillis(200));
        retries.add(() -> resetMembers().request(request, future, true, true));
      }
      return future;
    }

    // Remove the next random member from the members list.
    Address member = connectMembers.remove(random.nextInt(connectMembers.size()));

    // Connect to the server. If the connection fails, recursively attempt to connect to the next server,
    // otherwise setup the connection and send the request.
    if (connectFuture == null) {
      // If there's no existing connect future, create a new one.
      LOGGER.info("Connecting: {}", member.socketAddress());
      connectFuture = client.connect(member).whenComplete((connection, error) -> {
        connectFuture = null;
        if (!checkOpen || isOpen()) {
          if (error == null) {
            setupConnection(connection);
            request(request, connection, future, checkOpen, recordFailures);
          } else {
            LOGGER.info("Failed to connect: {}", member.socketAddress());
            resetConnection().request(request, future, checkOpen, recordFailures);
          }
        } else {
          future.completeExceptionally(new IllegalStateException("session not open"));
        }
      });
    } else {
      // We don't want concurrent requests to attempt to connect to the same server at the same time, so
      // if the connection is already being attempted, piggyback on the existing connect future.
      connectFuture.whenComplete((connection, error) -> {
        if (!checkOpen || isOpen()) {
          if (error == null) {
            request(request, connection, future, checkOpen, recordFailures);
          } else {
            request(request, future, checkOpen, recordFailures);
          }
        } else {
          future.completeExceptionally(new IllegalStateException("session not open"));
        }
      });
    }
    return future;
  }

  /**
   * Sends a session request to the given connection.
   *
   * @param request The request to send.
   * @param connection The connection to which to send the request.
   * @param future The future to complete once the response is received.
   * @param checkOpen Whether to check if the session is open.
   * @param <T> The request type.
   * @param <U> The response type.
   * @return The provided future to be completed once the response is received.
   */
  private <T extends Request<T>, U extends Response<U>> CompletableFuture<U> request(T request, Connection connection, CompletableFuture<U> future, boolean checkOpen, boolean recordFailures) {
    // Always acquire an additional reference prior to sending a request. The request reference will be released before the response is received.
    request.acquire();

    LOGGER.debug("Sending: {}", request);
    connection.<T, U>send(request).whenComplete((response, error) -> {
      if (!checkOpen || isOpen()) {
        if (error == null) {
          LOGGER.debug("Received: {}", response);

          // If the response is an error response, check if the session state has changed.
          if (response.status() == Response.Status.ERROR) {
            // If the response error is a no leader error, reset the connection and send another request.
            // If this is the first time we've received a response from a server in this iteration,
            // set the failure time to keep track of whether the session timed out.
            if (response.error() == RaftError.Type.NO_LEADER_ERROR) {
              if (recordFailures)
                setFailureTime();
              resetConnection().request(request, future, checkOpen, false);
            }
            // If the response error is an unknown session error, immediately expire the session.
            else if (response.error() == RaftError.Type.UNKNOWN_SESSION_ERROR) {
              resetConnection().onExpire();
              future.completeExceptionally(new IllegalStateException("session expired"));
            }
            // If the response error is an application or internal error, immediately complete the future.
            else if (response.error() == RaftError.Type.APPLICATION_ERROR
              || response.error() == RaftError.Type.INTERNAL_ERROR) {
              resetFailureTime();
              future.completeExceptionally(response.error().createException());
            }
            // If we've made it this far, for all other error types attempt to resend the request.
            else {
              resetFailureTime().resetConnection().request(request, future, checkOpen, false);
            }
          }
          // If the response status is OK, reset the failure time and complete the future.
          else {
            resetFailureTime();
            future.complete(response);
          }
        }
        // If an error occurred, attempt to contact the next server recursively.
        else {
          LOGGER.debug("Request timed out: {}", request);
          resetConnection().request(request, future, checkOpen, recordFailures);
        }
      } else {
        future.completeExceptionally(new IllegalStateException("session not open"));
      }
    });
    return future;
  }

  /**
   * Retries sending requests.
   */
  private void retryRequests() {
    retryFuture = null;
    List<Runnable> retries = new ArrayList<>(this.retries);
    this.retries.clear();
    resetMembers();
    for (Runnable retry : retries) {
      retry.run();
    }
  }

  /**
   * Resets the current connection.
   */
  private ClientSession resetConnection() {
    connection = null;
    return this;
  }

  /**
   * Resets the members to which to connect.
   */
  private ClientSession resetMembers() {
    if (connectMembers.isEmpty() || connectMembers.size() < members.size() - 1) {
      connectMembers = new ArrayList<>(members);
    }
    return this;
  }

  /**
   * Sets the failure time if not already set.
   */
  private ClientSession setFailureTime() {
    if (failureTime == 0) {
      failureTime = System.currentTimeMillis();
    }
    return this;
  }

  /**
   * Resets the failure timeout.
   */
  private ClientSession resetFailureTime() {
    failureTime = 0;
    return this;
  }

  /**
   * Sets up the given connection.
   */
  private ClientSession setupConnection(Connection connection) {
    this.connection = connection;
    connection.closeListener(c -> {
      if (c.equals(this.connection)) {
        this.connection = null;
      }
    });
    connection.exceptionListener(c -> {
      if (c.equals(this.connection)) {
        this.connection = null;
      }
    });
    connection.handler(PublishRequest.class, this::handlePublish);
    return this;
  }

  /**
   * Registers the session.
   */
  private CompletableFuture<Void> register() {
    context.checkThread();

    CompletableFuture<Void> future = new CompletableFuture<>();

    RegisterRequest request = RegisterRequest.builder()
      .withConnection(client.id())
      .build();

    request.acquire();
    this.<RegisterRequest, RegisterResponse>request(request, new CompletableFuture<>(), false, true).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          setMembers(response.members());
          setTimeout(response.timeout());
          onOpen(response.session());
          future.complete(null);
          resetMembers().keepAlive();
        } else {
          future.completeExceptionally(response.error().createException());
        }
        response.release();
      } else {
        future.completeExceptionally(error);
      }
      request.release();
    });
    return future;
  }

  /**
   * Sends and reschedules keep alive request.
   */
  private void keepAlive() {
    keepAliveFuture = context.schedule(() -> {
      if (isOpen()) {
        context.checkThread();

        KeepAliveRequest request = KeepAliveRequest.builder()
          .withSession(id)
          .withCommandSequence(responseSequence)
          .withEventVersion(eventVersion)
          .withEventSequence(eventSequence)
          .build();

        request.acquire();
        this.<KeepAliveRequest, KeepAliveResponse>request(request).whenComplete((response, error) -> {
          if (error == null) {
            if (response.status() == Response.Status.OK) {
              setMembers(response.members());
              resetMembers().keepAlive();
            } else if (isOpen()) {
              keepAlive();
            }
            response.release();
          } else if (isOpen()) {
            keepAlive();
          }
          request.release();
        });
      }
    }, keepAliveInterval);
  }

  /**
   * Triggers opening the session.
   */
  private void onOpen(long sessionId) {
    LOGGER.debug("Registered session: {}", sessionId);
    this.id = sessionId;
    this.state = State.OPEN;
    for (Consumer<Session> listener : openListeners) {
      listener.accept(this);
    }
  }

  @Override
  public CompletableFuture<Session> open() {
    CompletableFuture<Session> future = new CompletableFuture<>();
    context.executor().execute(() -> register().whenComplete((result, error) -> {
      if (error == null) {
        this.state = State.OPEN;
        future.complete(this);
      } else {
        future.completeExceptionally(error);
      }
    }));
    return future;
  }

  @Override
  public boolean isOpen() {
    return state == State.OPEN;
  }

  @Override
  public Listener<Session> onOpen(Consumer<Session> listener) {
    return openListeners.add(Assert.notNull(listener, "listener"));
  }

  @Override
  public CompletableFuture<Void> publish(Object message) {
    Assert.notNull(message, "message");
    return CompletableFuture.runAsync(() -> {
      for (Consumer<Object> listener : receiveListeners) {
        listener.accept(message);
      }
    }, context.executor());
  }

  /**
   * Handles a publish request.
   *
   * @param request The publish request to handle.
   * @return A completable future to be completed with the publish response.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<PublishResponse> handlePublish(PublishRequest request) {
    if (request.session() != id)
      return Futures.exceptionalFuture(new UnknownSessionException("incorrect session ID"));

    if (request.previousVersion() != eventVersion || request.previousSequence() != eventSequence) {
      return CompletableFuture.completedFuture(PublishResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withVersion(eventVersion)
        .withSequence(eventSequence)
        .build());
    }

    eventVersion = request.eventVersion();
    eventSequence = request.eventSequence();

    for (Consumer listener : receiveListeners) {
      listener.accept(request.message());
    }

    request.release();

    return CompletableFuture.completedFuture(PublishResponse.builder()
      .withStatus(Response.Status.OK)
      .withVersion(eventVersion)
      .withSequence(eventSequence)
      .build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Listener<?> onReceive(Consumer listener) {
    return receiveListeners.add(Assert.notNull(listener, "listener"));
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.runAsync(() -> {
      if (keepAliveFuture != null) {
        keepAliveFuture.cancel();
      }
      if (retryFuture != null) {
        retryFuture.cancel();
      }
      onClose();
    }, context.executor());
  }

  /**
   * Handles closing the session.
   */
  private void onClose() {
    if (isOpen()) {
      LOGGER.debug("Closed session: {}", id);
      this.id = 0;
      this.state = State.CLOSED;
      if (connection != null)
        connection.close();
      client.close();
      context.close();
      closeListeners.forEach(l -> l.accept(this));
    }
  }

  @Override
  public boolean isClosed() {
    return state == State.CLOSED || state == State.EXPIRED;
  }

  @Override
  public Listener<Session> onClose(Consumer<Session> listener) {
    return closeListeners.add(Assert.notNull(listener, "listener"));
  }

  /**
   * Handles expiring the session.
   */
  private void onExpire() {
    if (isOpen()) {
      LOGGER.debug("Expired session: {}", id);
      this.id = 0;
      this.state = State.EXPIRED;
      closeListeners.forEach(l -> l.accept(this));
    }
  }

  @Override
  public boolean isExpired() {
    return state == State.EXPIRED;
  }

}
