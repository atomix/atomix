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

import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.LocalMember;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.cluster.coordinator.LocalMemberCoordinator;
import net.kuujo.copycat.cluster.manager.LocalMemberManager;
import net.kuujo.copycat.util.serializer.Serializer;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Internal local cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedLocalMember extends CoordinatedMember implements LocalMemberManager {
  private final LocalMemberCoordinator coordinator;
  private boolean open;

  public CoordinatedLocalMember(int id, MemberInfo info, LocalMemberCoordinator coordinator, Serializer serializer, Executor executor) {
    super(id, info, coordinator, serializer, executor);
    this.coordinator = coordinator;
  }

  /**
   * Wraps a message handler in order to perform serialization/deserialization and execute it in the proper thread.
   */
  private <T, U> MessageHandler<ByteBuffer, ByteBuffer> wrapHandler(MessageHandler<T, U> handler, Serializer serializer) {
    return message -> {
      CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
      executor.execute(() -> {
        handler.handle(serializer.readObject(message)).whenComplete((result, error) -> {
          if (error == null) {
            future.complete(serializer.writeObject(result));
          } else {
            future.completeExceptionally(error);
          }
        });
      });
      return future;
    };
  }

  @Override
  public <T, U> LocalMemberManager registerHandler(String topic, int id, MessageHandler<T, U> handler, Serializer serializer) {
    coordinator.register(topic, this.id, id, wrapHandler(handler, serializer));
    return this;
  }

  @Override
  public LocalMemberManager unregisterHandler(String topic, int id) {
    coordinator.unregister(topic, this.id, id);
    return this;
  }

  @Override
  public <T, U> LocalMember registerHandler(String topic, MessageHandler<T, U> handler) {
    return registerHandler(topic, USER_ID, handler, serializer);
  }
  
  @Override
  public LocalMember unregisterHandler(String topic) {
    return unregisterHandler(topic, USER_ID);
  }

  @Override
  public CompletableFuture<LocalMemberManager> open() {
    open = true;
    this.<Task<?>, Object>registerHandler(EXECUTE_TOPIC, EXECUTE_ID, task -> CompletableFuture.supplyAsync(task::execute, executor), serializer);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    unregisterHandler(EXECUTE_TOPIC, EXECUTE_ID);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
