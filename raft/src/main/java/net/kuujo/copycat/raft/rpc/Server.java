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
package net.kuujo.copycat.raft.rpc;

import net.kuujo.copycat.raft.Member;

import java.util.concurrent.CompletableFuture;

/**
 * RPC server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Server {

  /**
   * Returns the server ID.
   *
   * @return The server ID.
   */
  int id();

  /**
   * Listens for connections on the server.
   *
   * @param listener The connection listener.
   * @return A completable future to be called once the server has started listening for connections.
   */
  CompletableFuture<Void> listen(Member member, ConnectionListener listener);

  /**
   * Closes the server.
   *
   * @return A completable future to be completed once the server is closed.
   */
  CompletableFuture<Void> close();

}
