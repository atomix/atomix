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
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.coordinator.CoordinatedResourceConfig;
import net.kuujo.copycat.cluster.manager.ClusterManager;
import net.kuujo.copycat.log.LogManager;
import net.kuujo.copycat.protocol.Consistency;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * Copycat resource context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ResourceContext extends Managed<ResourceContext>, Executor {

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
