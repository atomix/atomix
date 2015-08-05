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
package net.kuujo.copycat.raft.client;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Listeners;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.transport.Client;
import net.kuujo.copycat.io.transport.Connection;
import net.kuujo.copycat.io.transport.Transport;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.protocol.*;
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
  private static enum State {
    OPEN,
    CLOSED,
    EXPIRED
  }

  private final Random random = new Random();
  private final Client client;
  private Members members;
  private final long keepAliveInterval;
  private final Context context;
  private List<Member> connectMembers;
  private Connection connection;
  private volatile State state = State.CLOSED;
  private volatile long id;
  private ScheduledFuture<?> keepAliveFuture;
  private final Listeners<Session> openListeners = new Listeners<>();
  private final Listeners<Object> receiveListeners = new Listeners<>();
  private final Listeners<Session> closeListeners = new Listeners<>();
  private long requestSequence;
  private long responseSequence;
  private long eventSequence;

  ClientSession(Transport transport, Members members, long keepAliveInterval, Serializer serializer) {
    UUID id = UUID.randomUUID();
    this.client = transport.client(id);
    this.members = members;
    this.keepAliveInterval = keepAliveInterval;
    this.context = new SingleThreadContext("copycat-client-" + id.toString(), serializer.clone());
    this.connectMembers = new ArrayList<>(members.members());
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
  private void setMembers(Members members) {
    this.members = members;
    this.connectMembers = new ArrayList<>(this.members.members());
  }

  /**
   * Submits a command via the session.
   *
   * @param command The command to submit.
   * @param <T> The command output type.
   * @return A completable future to be completed with the command output.
   */
  public <T> CompletableFuture<T> submit(Command<T> command) {
    CompletableFuture<T> future = new CompletableFuture<>();
    context.execute(() -> {

      CommandRequest request = CommandRequest.builder()
        .withSession(id)
        .withCommandSequence(++requestSequence)
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
    if (!isOpen())
      return Futures.exceptionalFuture(new IllegalStateException("session not open"));

    this.<CommandRequest, CommandResponse>request(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          responseSequence = Math.max(responseSequence, request.commandSequence());
          future.complete((T) response.result());
          resetMembers();
          request.close();
        } else {
          resetConnection().submit(request, future);
        }
        response.close();
      } else {
        future.completeExceptionally(error);
        request.close();
      }
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
    CompletableFuture<T> future = new CompletableFuture<>();
    context.execute(() -> {

      QueryRequest request = QueryRequest.builder()
        .withSession(id)
        .withCommandSequence(responseSequence)
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
    if (!isOpen())
      return Futures.exceptionalFuture(new IllegalStateException("session not open"));

    this.<QueryRequest, QueryResponse>request(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete((T) response.result());
          resetMembers();
          request.close();
        } else {
          resetConnection().submit(request, future);
        }
        response.close();
      } else {
        future.completeExceptionally(error);
        request.close();
      }
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
      return Futures.exceptionalFutureAsync(new IllegalStateException("session not open"), context);
    return request(request, new CompletableFuture<U>(), true);
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
  private <T extends Request<T>, U extends Response<U>> CompletableFuture<U> request(T request, CompletableFuture<U> future, boolean checkOpen) {
    context.checkThread();

    if (checkOpen && !isOpen()) {
      future.completeExceptionally(new IllegalStateException("session not open"));
      return future;
    }

    // If we're already connected to a server, use the existing connection. The connection will be reset in the event
    // of an error on any connection, or callers can reset connections as well.
    if (connection != null) {
      LOGGER.debug("Sending: {}", request);
      connection.<T, U>send(request).whenComplete((response, error) -> {
        if (!checkOpen || isOpen()) {
          if (error == null) {
            LOGGER.debug("Received: {}", response);
            future.complete(response);
          } else {
            resetConnection().request(request, future, checkOpen);
          }
        } else {
          future.completeExceptionally(new IllegalStateException("session not open"));
        }
      });
      return future;
    }

    // If we've run out of members to which to connect then expire the session.
    if (connectMembers.isEmpty()) {
      LOGGER.warn("Failed to connect to cluster");
      resetConnection().onExpire();
      future.completeExceptionally(new IllegalStateException("session not open"));
      return future;
    }

    Member member = connectMembers.remove(random.nextInt(connectMembers.size()));

    final InetSocketAddress address;
    try {
      address = new InetSocketAddress(InetAddress.getByName(member.host()), member.port());
    } catch (UnknownHostException e) {
      return Futures.exceptionalFuture(e);
    }

    LOGGER.info("Connecting: {}", address);
    client.connect(address).whenComplete((connection, connectError) -> {
      if (connectError == null) {
        setupConnection(connection);

        LOGGER.debug("Sending: {}", request);
        connection.<T, U>send(request).whenComplete((response, responseError) -> {
          if (!checkOpen || isOpen()) {
            if (responseError == null) {
              LOGGER.debug("Received: {}", response);
              future.complete(response);
            } else {
              resetConnection().request(request, future, checkOpen);
            }
          } else {
            future.completeExceptionally(new IllegalStateException("session not open"));
          }
        });
      } else {
        LOGGER.info("Failed to connect: {}", address);
        resetConnection().request(request, future, checkOpen);
      }
    });
    return future;
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
    if (connectMembers.isEmpty() || connectMembers.size() < members.members().size() - 1) {
      connectMembers = new ArrayList<>(members.members());
    }
    return this;
  }

  /**
   * Sets up the given connection.
   */
  private ClientSession setupConnection(Connection connection) {
    this.connection = connection;
    connection.closeListener(c -> this.connection = null);
    connection.exceptionListener(c -> this.connection = null);
    connection.handler(PublishRequest.class, this::handlePublish);
    return this;
  }

  /**
   * Registers the session.
   */
  private CompletableFuture<Void> register() {
    return register(new CompletableFuture<>());
  }

  /**
   * Recursively attempts to register the session.
   */
  private CompletableFuture<Void> register(CompletableFuture<Void> future) {
    context.checkThread();

    RegisterRequest request = RegisterRequest.builder()
      .withConnection(client.id())
      .build();

    this.<RegisterRequest, RegisterResponse>request(request, new CompletableFuture<>(), false).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          setMembers(response.members());
          onOpen(response.session());
          future.complete(null);
          resetMembers().keepAlive();
        } else {
          resetConnection().register(future);
        }
        response.close();
      } else {
        future.completeExceptionally(error);
      }
      request.close();
    });
    return future;
  }

  /**
   * Sends and reschedules keep alive request.
   */
  private void keepAlive() {
    keepAliveFuture = context.schedule(() -> {
      if (isOpen()) {
        keepAlive(new CompletableFuture<>()).thenRun(this::keepAlive);
      }
    }, keepAliveInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * Sends a keep alive request.
   */
  private CompletableFuture<Void> keepAlive(CompletableFuture<Void> future) {
    context.checkThread();

    KeepAliveRequest request = KeepAliveRequest.builder()
      .withSession(id)
      .withCommandSequence(responseSequence)
      .build();

    this.<KeepAliveRequest, KeepAliveResponse>request(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          setMembers(response.members());
          future.complete(null);
          resetMembers();
        } else {
          resetConnection().keepAlive(future);
        }
        response.close();
      } else {
        future.completeExceptionally(error);
      }
      request.close();
    });

    return future;
  }

  /**
   * Triggers opening the session.
   */
  private void onOpen(long sessionId) {
    LOGGER.debug("Registered session: {}", sessionId);
    this.id = sessionId;
    this.state = State.OPEN;
    for (Listener<Session> listener : openListeners) {
      listener.accept(this);
    }
  }

  @Override
  public CompletableFuture<Session> open() {
    CompletableFuture<Session> future = new CompletableFuture<>();
    context.execute(() -> {
      register().whenComplete((result, error) -> {
        if (error == null) {
          this.state = State.OPEN;
          future.complete(this);
        } else {
          future.completeExceptionally(error);
        }
      });
    });
    return future;
  }

  @Override
  public boolean isOpen() {
    return state == State.OPEN;
  }

  @Override
  public ListenerContext<Session> onOpen(Listener<Session> listener) {
    return openListeners.add(listener);
  }

  @Override
  public CompletableFuture<Void> publish(Object message) {
    return CompletableFuture.runAsync(() -> {
      for (Listener<Object> listener : receiveListeners) {
        listener.accept(message);
      }
    }, context);
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

    if (request.eventSequence() < eventSequence || request.eventSequence() > eventSequence + 1) {
      return CompletableFuture.completedFuture(PublishResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withEventSequence(eventSequence)
        .build());
    }

    eventSequence = request.eventSequence();

    for (Listener listener : receiveListeners) {
      listener.accept(request.message());
    }

    return CompletableFuture.completedFuture(PublishResponse.builder()
      .withStatus(Response.Status.OK)
      .withEventSequence(eventSequence)
      .build());
  }

  @Override
  @SuppressWarnings("unchecked")
  public ListenerContext<?> onReceive(Listener listener) {
    return receiveListeners.add(listener);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.runAsync(() -> {
      if (keepAliveFuture != null) {
        keepAliveFuture.cancel(false);
      }
      onClose();
    }, context);
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
  public ListenerContext<Session> onClose(Listener<Session> listener) {
    return closeListeners.add(listener);
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
