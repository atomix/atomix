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
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.Managed;
import net.kuujo.copycat.cluster.LocalMember;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.cluster.coordinator.LocalMemberCoordinator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Internal local cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedLocalMember extends CoordinatedMember implements LocalMember, Managed<CoordinatedLocalMember> {
  private final LocalMemberCoordinator coordinator;
  private boolean open;

  public CoordinatedLocalMember(int id, MemberInfo info, LocalMemberCoordinator coordinator, Executor executor) {
    super(id, info, coordinator, executor);
    this.coordinator = coordinator;
  }

  @Override
  public synchronized <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler) {
    coordinator.register(topic, id, handler);
    return this;
  }
  
  @Override
  public synchronized LocalMember unregisterHandler(String topic) {
    coordinator.unregister(topic, id);
    return this;
  }

  @Override
  public CompletableFuture<CoordinatedLocalMember> open() {
    open = true;
    coordinator.registerExecutor(id, executor);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    coordinator.unregisterExecutor(id);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
