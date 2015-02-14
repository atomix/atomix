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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.protocol.ProtocolConnection;
import net.kuujo.copycat.protocol.ProtocolServer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Coordinates protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedProtocolServer implements ProtocolServer {
  private final int id;
  private final ProtocolServerCoordinator coordinator;
  private final Executor executor;

  public CoordinatedProtocolServer(int id, ProtocolServerCoordinator coordinator, Executor executor) {
    this.id = id;
    this.coordinator = coordinator;
    this.executor = executor;
  }

  @Override
  public CompletableFuture<Void> listen() {
    return coordinator.listen();
  }

  @Override
  public ProtocolServer connectListener(EventListener<ProtocolConnection> handler) {
    coordinator.connectListener(id, handler, executor);
    return this;
  }

  @Override
  public CompletableFuture<Void> close() {
    return coordinator.close();
  }

}
