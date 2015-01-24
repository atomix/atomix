/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.resource.internal;

import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.cluster.internal.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.cluster.internal.manager.ClusterManager;
import net.kuujo.copycat.log.LogManager;
import net.kuujo.copycat.protocol.Consistency;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Copycat resource context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ResourceContext extends Managed<ResourceContext> {

  /**
   * Returns the resource name.
   *
   * @return The resource name.
   */
  String name();

  /**
   * Returns the resource configuration.
   *
   * @return The resource configuration.
   */
  CoordinatedResourceConfig config();

  /**
   * Returns the current Copycat state.
   *
   * @return The current Copycat state.
   */
  CopycatState state();

  /**
   * Returns the Copycat cluster.
   *
   * @return The Copycat cluster.
   */
  ClusterManager cluster();

  /**
   * Returns the Copycat log.
   *
   * @return The Copycat log.
   */
  LogManager log();

  /**
   * Registers an entry consumer on the context.
   *
   * @param consumer The entry consumer.
   * @return The Copycat context.
   */
  ResourceContext consumer(BiFunction<Long, ByteBuffer, ByteBuffer> consumer);

  /**
   * Executes a command on the context.
   *
   * @param command The command to execute.
   */
  void execute(Runnable command);

  /**
   * Schedules a command on the context.
   *
   * @param command The command to schedule.
   * @param delay The delay after which to run the command.
   * @param unit The delay time unit.
   * @return The scheduled future.
   */
  ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

  /**
   * Schedules a callable on the context.
   *
   * @param callable The callable to schedule.
   * @param delay The delay after which to run the callable.
   * @param unit The delay time unit.
   * @return The scheduled future.
   */
  <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit);

  /**
   * Schedules a command to run at a fixed rate on the context.
   *
   * @param command The command to schedule.
   * @param initialDelay The initial delay after which to execute the command for the first time.
   * @param period The period at which to run the command.
   * @param unit The period time unit.
   * @return The scheduled future.
   */
  ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);

  /**
   * Schedules a command to run at a fixed delay on the context.
   *
   * @param command The command to schedule.
   * @param initialDelay The initial delay after which to execute the command for the first time.
   * @param delay The delay at which to run the command.
   * @param unit The delay time unit.
   * @return The scheduled future.
   */
  ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit);

  /**
   * Submits a persistent entry to the context.
   *
   * @param entry The entry to commit.
   * @return A completable future to be completed once the entry has been committed.
   */
  CompletableFuture<ByteBuffer> commit(ByteBuffer entry);

  /**
   * Submits a synchronous entry to the context.
   *
   * @param entry The entry to query.
   * @return A completable future to be completed once the cluster has been synchronized.
   */
  CompletableFuture<ByteBuffer> query(ByteBuffer entry);

  /**
   * Submits a synchronous entry to the context.
   *
   * @param entry The entry to query.
   * @return A completable future to be completed once the cluster has been synchronized.
   */
  CompletableFuture<ByteBuffer> query(ByteBuffer entry, Consistency consistency);

}
