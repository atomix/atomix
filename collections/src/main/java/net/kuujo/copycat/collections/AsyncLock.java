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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.CopycatResource;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.internal.lock.AsyncLockState;
import net.kuujo.copycat.collections.internal.lock.DefaultAsyncLock;
import net.kuujo.copycat.collections.internal.lock.UnlockedAsyncLockState;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.spi.Protocol;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous lock.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncLock extends CopycatResource {

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @return A new asynchronous lock.
   */
  static AsyncLock create(String name) {
    return create(name, Services.load("cluster"), Services.load("protocol"));
  }

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @param config The cluster configuration.
   * @param protocol The cluster protocol.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name, ClusterConfig config, Protocol protocol) {
    return new DefaultAsyncLock(StateMachine.<AsyncLockState>create(name, AsyncLockState.class, new UnlockedAsyncLockState(), config, protocol));
  }

  /**
   * Acquires the log.
   *
   * @return A completable future to be completed once the lock has been acquired.
   */
  CompletableFuture<Void> lock();

  /**
   * Releases the lock.
   *
   * @return A completable future to be completed once the lock has been released.
   */
  CompletableFuture<Void> unlock();

}
