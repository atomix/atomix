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
package net.kuujo.copycat.raft.transport;

import java.util.concurrent.CompletableFuture;

/**
 * Transport layer message handler.
 * <p>
 * This is a functional interface for handling messages on a {@link Connection} instance.
 * Message handlers are {@link Connection#handler(Class, MessageHandler) registered} for specific types. When
 * {@link MessageHandler#handle(Object)} is invoked by a {@link Connection}, the handler
 * <em>must</em> return a valid {@link java.util.concurrent.CompletableFuture} to be completed with the message reply.
 * <p>
 * Note that input and output for the message handler must be serializable via the configured {@link net.kuujo.copycat.io.serializer.Serializer}
 * instance. This means it must implement {@link java.io.Serializable}, {@link java.io.Externalizable}, or
 * {@link net.kuujo.copycat.io.serializer.CopycatSerializable} or provide a custom {@link net.kuujo.copycat.io.serializer.TypeSerializer}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@FunctionalInterface
public interface MessageHandler<T, U> {

  /**
   * Handles a message.
   * <p>
   * The handler must synchronously return a {@link java.util.concurrent.CompletableFuture} to be completed with the
   * message response. If the message is handled synchronously, return {@link CompletableFuture#completedFuture(Object)}
   * to immediately complete the returned future. Otherwise, create a {@link java.util.concurrent.CompletableFuture}
   * and call {@link java.util.concurrent.CompletableFuture#complete(Object)} or
   * {@link java.util.concurrent.CompletableFuture#completeExceptionally(Throwable)} to complete the future with a reply
   * or error respectively.
   *
   * @param message The message to handle.
   * @return A completable future to be completed with the message response.
   */
  CompletableFuture<U> handle(T message);

}
