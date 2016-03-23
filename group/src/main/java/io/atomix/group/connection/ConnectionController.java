/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.connection;

import io.atomix.catalyst.util.Assert;

import java.util.concurrent.CompletableFuture;

/**
 * Member connection controller.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ConnectionController {
  private final LocalConnection connection;

  public ConnectionController(LocalConnection connection) {
    this.connection = Assert.notNull(connection, "connection");
  }

  /**
   * Returns the underlying connection.
   *
   * @return The underlying connection.
   */
  public LocalConnection connection() {
    return connection;
  }

  /**
   * Called when a message is received.
   *
   * @param message The received message.
   * @return A completable future to be completed with the message response.
   */
  public CompletableFuture<Object> onMessage(Message<?> message) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    connection.onMessage(message.setFuture(future));
    return future;
  }

}
