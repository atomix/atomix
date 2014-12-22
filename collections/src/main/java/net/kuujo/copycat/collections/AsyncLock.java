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
import net.kuujo.copycat.spi.ExecutionContext;

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
   * @param uri The asynchronous lock member URI.
   * @return A new asynchronous lock.
   */
  static AsyncLock create(String name, String uri) {
    return create(name, uri, new ClusterConfig(), new AsyncLockConfig(String.format("copycat.lock.%s", name)), ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @param uri The asynchronous lock member URI.
   * @param cluster The cluster configuration.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name, String uri, ClusterConfig cluster) {
    return create(name, uri, cluster, new AsyncLockConfig(String.format("copycat.lock.%s", name)), ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @param uri The asynchronous lock member URI.
   * @param config The lock configuration.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name, String uri, AsyncLockConfig config) {
    return create(name, uri, new ClusterConfig(), config, ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @param uri The asynchronous lock member URI.
   * @param context The user execution context.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name, String uri, ExecutionContext context) {
    return create(name, uri, new ClusterConfig(), new AsyncLockConfig(String.format("copycat.lock.%s", name)), context);
  }

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @param uri The asynchronous lock member URI.
   * @param cluster The cluster configuration.
   * @param config The lock configuration.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name, String uri, ClusterConfig cluster, AsyncLockConfig config) {
    return create(name, uri, cluster, config, ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @param uri The asynchronous lock member URI.
   * @param cluster The cluster configuration.
   * @param context The user execution context.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name, String uri, ClusterConfig cluster, ExecutionContext context) {
    return create(name, uri, cluster, new AsyncLockConfig(String.format("copycat.lock.%s", name)), context);
  }

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @param uri The asynchronous lock member URI.
   * @param config The lock configuration.
   * @param context The user execution context.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name, String uri, AsyncLockConfig config, ExecutionContext context) {
    return create(name, uri, new ClusterConfig(), config, context);
  }

  /**
   * Creates a new asynchronous lock.
   *
   * @param name The asynchronous lock name.
   * @param uri The asynchronous lock member URI.
   * @param cluster The cluster configuration.
   * @param config The lock configuration.
   * @param context The user execution context.
   * @return The asynchronous lock.
   */
  static AsyncLock create(String name, String uri, ClusterConfig cluster, AsyncLockConfig config, ExecutionContext context) {
    return new DefaultAsyncLock(StateMachine.create(name, uri, AsyncLockState.class, new UnlockedAsyncLockState(), cluster, config, context));
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
