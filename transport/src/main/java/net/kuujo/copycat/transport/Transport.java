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
package net.kuujo.copycat.transport;

import java.util.concurrent.CompletableFuture;

/**
 * Transport.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Transport {

  /**
   * Creates a transport client.
   *
   * @param id The client ID.
   * @return The transport client.
   */
  Client client(int id);

  /**
   * Creates a transport server.
   *
   * @param id The server ID.
   * @return The transport server.
   */
  Server server(int id);

  /**
   * Closes the transport.
   *
   * @return A completable future to be completed once the transport is closed.
   */
  CompletableFuture<Void> close();

}
