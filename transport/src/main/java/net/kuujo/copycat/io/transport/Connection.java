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
package net.kuujo.copycat.io.transport;

import net.kuujo.copycat.util.Listener;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Transport connection.
 * <p>
 * This is a low-level abstraction through which clients and servers communicate with one another once connected.
 * This is more or less a lightweight interface over sockets that supports arbitrary messages.
 * <p>
 * Messages sent over a connection must be serializable by the registered {@link net.kuujo.copycat.io.serializer.Serializer}.
 * This means that messages must implement {@link java.io.Serializable}, {@link java.io.Externalizable}, or
 * {@link net.kuujo.copycat.io.serializer.CopycatSerializable} or provide a custom {@link net.kuujo.copycat.io.serializer.TypeSerializer}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Connection {

  /**
   * Returns the connection ID.
   * <p>
   * The connection ID is inherited from the {@link Client} that is connected. When the connection
   * is created, the client will send its {@link Client#id()} to the {@link Server}
   * and the connection ID will reflect the client's ID on both the client and server side.
   *
   * @return The connection ID.
   */
  UUID id();

  /**
   * Sends a message to the other side of the connection.
   * <p>
   * The message must be serializable via the configured {@link net.kuujo.copycat.io.serializer.Serializer} instance. This means it
   * must implement {@link java.io.Serializable}, {@link java.io.Externalizable}, or {@link net.kuujo.copycat.io.serializer.CopycatSerializable}
   * or provide a custom {@link net.kuujo.copycat.io.serializer.TypeSerializer}.
   * <p>
   * Note that {@link Connection}s are bi-directional. That is, messages can be send either
   * by the client or the server. All messages must have a reply, even if the reply is {@code null}. Once the reply
   * has been received from the other side of the connection, the returned {@link java.util.concurrent.CompletableFuture}
   * will be completed.
   * <p>
   * {@link Connection} implementations must guarantee that all reply
   * {@link java.util.concurrent.CompletableFuture futures} will be completed in the same
   * {@link net.kuujo.copycat.util.concurrent.CopycatThread Copycat thread}.
   *
   * @param message The message to send.
   * @param <T> The message type.
   * @param <U> The reply type.
   * @return A completable future to be completed with the response.
   */
  <T, U> CompletableFuture<U> send(T message);

  /**
   * Sets a message handler on the connection.
   * <p>
   * The message handler will be invoked each time a message of the given type is received from the other side of the
   * connection. All messages are classified by type, and only one {@link MessageHandler} may
   * be registered on the connection for any given type.
   * <p>
   * The message handler must return a {@link java.util.concurrent.CompletableFuture} to be completed with the message
   * reply. The reply value must be serializable via the configured {@link net.kuujo.copycat.io.serializer.Serializer} instance. This means it
   * must implement {@link java.io.Serializable}, {@link java.io.Externalizable}, or {@link net.kuujo.copycat.io.serializer.CopycatSerializable}
   * or provide a custom {@link net.kuujo.copycat.io.serializer.TypeSerializer}.
   *
   * @param type The message type for which to listen. This can be any class that is serializable by the configured
   * {@link net.kuujo.copycat.io.serializer.Serializer} instance.
   * @param handler The type-specific message handler.
   * @param <T> The message type.
   * @param <U> The reply type.
   */
  <T, U> Connection handler(Class<T> type, MessageHandler<T, U> handler);

  /**
   * Sets an exception listener on the connection.
   * <p>
   * In the event of an exception in the connection, the provided listener's {@link Consumer#accept(Object)} method will
   * be invoked. To unregister the listener, simply {@link Listener#close()} the returned
   * {@link Listener}.
   *
   * @param listener The exception listener.
   * @return The connection.
   */
  Listener<Throwable> exceptionListener(Consumer<Throwable> listener);

  /**
   * Sets a close listener on the connection.
   * <p>
   * The provided listener's {@link Consumer#accept(Object)} method will be invoked when the connection is closed. Note
   * that a close event can be triggered via {@link Connection#close()} or by the
   * {@link Client} or {@link Server} that created the connection.
   *
   * @param listener The close listener.
   * @return The connection.
   */
  Listener<Connection> closeListener(Consumer<Connection> listener);

  /**
   * Closes the connection.
   * <p>
   * Once the connection is closed, no more messages can be {@link Connection#send(Object) sent} or
   * {@link Connection#handler(Class, MessageHandler) received} by the connection. Any
   * {@link Connection#closeListener(Consumer) close listeners} registered on the connection will be
   * invoked, and the returned {@link java.util.concurrent.CompletableFuture} will be completed once the connection has
   * been closed.
   *
   * @return A completable future to be completed once the connection is closed.
   */
  CompletableFuture<Void> close();

}
