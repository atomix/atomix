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
package net.kuujo.copycat.raft.protocol;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;

import java.util.concurrent.CompletableFuture;

/**
 * Client/server connection.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Connection {

  /**
   * Returns the connection ID.
   *
   * @return The connection ID.
   */
  int id();

  /**
   * Sends a request.
   *
   * @param request The request to send.
   * @param <T> The request type.
   * @param <U> The response type.
   * @return A completable future to be completed with the response.
   */
  <T extends Request<T>, U extends Response<U>> CompletableFuture<U> send(T request);

  /**
   * Sets a request handler on the connection.
   *
   * @param type The request type for which to listen.
   * @param handler The request handler.
   * @param <T> The request type.
   * @param <U> The response type.
   */
  <T extends Request<T>, U extends Response<U>> Connection handler(Class<T> type, RequestHandler<T, U> handler);

  /**
   * Sets an exception listener on the connection.
   *
   * @param listener The exception listener.
   * @return The connection.
   */
  ListenerContext<Throwable> exceptionListener(Listener<Throwable> listener);

  /**
   * Sets a close listener on the connection.
   *
   * @param listener The close listener.
   * @return The connection.
   */
  ListenerContext<Connection> closeListener(Listener<Connection> listener);

  /**
   * Closes the connection.
   *
   * @return A completable future to be completed once the connection is closed.
   */
  CompletableFuture<Void> close();

}
