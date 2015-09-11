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

import net.kuujo.copycat.util.Listener;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Provides event-based methods for monitoring Raft sessions and communicating between Raft clients and servers.
 * <p>
 * Each client or server connected to any server in a Raft cluster must open a {@link Session}.
 * Sessions can be used by both clients and servers to monitor the connection status of another client or server. When
 * a client first connects to a server, it must register a new session. Once the session has been registered, listeners
 * registered via {@link #onOpen(Consumer)} will be called on <em>both the client and server side</em>. Thereafter, the
 * session can be used to {@link #publish(Object)} and {@link #onEvent(Consumer) receive} events between client and
 * server.
 * <p>
 * Sessions represent a connection between a single client and all servers in a Raft cluster. Session information
 * is replicated via the Raft consensus algorithm, and clients can safely switch connections between servers without
 * losing their session. Session implementations guarantee linearizability for session messages by coordinating
 * between the client and a single server at any given time. This means messages {@link #publish(Object) published}
 * via the {@link Session} are guaranteed to arrive on the other side of the connection exactly once and in the order
 * in which they are sent. In the event of a server-to-client message being lost, the message will be resent so long
 * as at least one Raft server is able to communicate with the client and the client's session does not {@link #isExpired()
 * expire} while switching between servers.
 * <p>
 * Messages are sent to the other side of the session using the {@link #publish(Object)} method:
 * <pre>
 *   {@code
 *     session.publish("Hello world!");
 *   }
 * </pre>
 * When the message is published, it will be queued to be sent to the other side of the connection. Copycat guarantees
 * that the message will arrive within the session timeout unless the session itself times out.
 * <p>
 * To listen for events on a session register a {@link Consumer} via {@link #onEvent(Consumer)}:
 * <pre>
 *   {@code
 *     session.onEvent(message -> System.out.println("Received: " + message));
 *   }
 * </pre>
 * Messages will always be received in the same thread and in the order in which they were sent by the other side of
 * the connection. Note that messages can be sent and received from the client or server side of the connection.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Session {

  /**
   * Returns the session ID.
   * <p>
   * The session ID is unique to an individual session within the cluster. That is, it is guaranteed that
   * no two clients will have a session with the same ID.
   *
   * @return The session ID.
   */
  long id();

  /**
   * Returns a boolean value indicating whether the session is open.
   *
   * @return Indicates whether the session is open.
   */
  boolean isOpen();

  /**
   * Sets an open listener on the session.
   * <p>
   * The provided {@link Consumer} will be {@link Consumer#accept(Object) called} once the session has
   * been registered with the Raft cluster. If the session is already {@link #isOpen() open} at the time
   * of this method call, the provided {@link Consumer} will be immediately called in the session thread.
   *
   * @param listener The session open listener.
   * @return The listener context.
   * @throws NullPointerException if {@code listener} is null
   */
  Listener<Session> onOpen(Consumer<Session> listener);

  /**
   * Publishes an event to the session.
   * <p>
   * When an event is published via the {@link Session}, it is sent to the other side of the session's
   * connection. If the event is sent from the client-side of the session, the event will be handled on
   * the client side as well. Sessions guarantee serializable consistency. If an event is sent from a Raft
   * server to a client that is disconnected or otherwise can't receive the event, the event will be resent
   * once the client connects to another server as long as its session has not {@link #isExpired() expired}.
   * <p>
   * The returned {@link CompletableFuture} will be completed once the {@code event} has been sent
   * but not necessarily received by the other side of the connection. In the event of a network or other
   * failure, the message may be resent.
   *
   * @param event The event to publish.
   * @return A completable future to be called once the event has been published.
   */
  CompletableFuture<Void> publish(Object event);

  /**
   * Registers an event listener.
   * <p>
   * The registered {@link Consumer} will be {@link Consumer#accept(Object) called} when an event is received
   * from the Raft cluster for the session. {@link Session} implementations must guarantee that consumers are
   * always called in the same thread for the session. Therefore, no two events will be received concurrently
   * by the session.
   *
   * @param listener The session receive listener.
   * @param <T> The session event type.
   * @return The listener context.
   * @throws NullPointerException if {@code listener} is null
   */
  <T> Listener<T> onEvent(Consumer<T> listener);

  /**
   * Sets a session close listener.
   * <p>
   * The registered close {@link Consumer} will be {@link Consumer#accept(Object) called} once the session
   * is closed either via an explicit invocation of a {@link net.kuujo.copycat.util.Managed} session or
   * by a session {@link #isExpired() expiration}. If the session was closed normally, {@link #isClosed()}
   * will return {@code true} and {@link #isExpired()} will {@code false}, otherwise if the session expired
   * then both will return {@code true}.
   *
   * @param listener The session close listener.
   * @return The session.
   * @throws NullPointerException if {@code listener} is null
   */
  Listener<Session> onClose(Consumer<Session> listener);

  /**
   * Returns a boolean value indicating whether the session is closed.
   * <p>
   * This method will return {@code true} if the session was closed via normal means or via a session
   * {@link #isExpired() expiration}. To determine whether the session expired use {@link #isExpired()}.
   *
   * @return Indicates whether the session is closed.
   */
  boolean isClosed();

  /**
   * Returns a boolean value indicating whether the session is expired.
   *
   * @return Indicates whether the session is expired.
   */
  boolean isExpired();

}
