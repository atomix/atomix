/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.EventListener;

import java.util.concurrent.CompletableFuture;

/**
 * Protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ProtocolServer {

  /**
   * Returns the server address.
   *
   * @return The server address.
   */
  String address();

  /**
   * Starts the server.
   *
   * @return A completable future to be completed once the server is started.
   */
  CompletableFuture<Void> listen();

  /**
   * Registers a server connection listener.
   *
   * @param listener A server connection listener.
   * @return The protocol server.
   */
  ProtocolServer connectListener(EventListener<ProtocolConnection> listener);

  /**
   * Closes the server.
   *
   * @return A completable future to be completed once the server is closed.
   */
  CompletableFuture<Void> close();

}
