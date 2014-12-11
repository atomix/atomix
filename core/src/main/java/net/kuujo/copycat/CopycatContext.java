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
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;

/**
 * Raft context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CopycatContext extends Managed {

  /**
   * Returns the Copycat cluster.
   *
   * @return The Copycat cluster.
   */
  Cluster cluster();

  /**
   * Returns the current Copycat state.
   *
   * @return The current Copycat state.
   */
  CopycatState state();

  /**
   * Returns the Copycat execution context.
   *
   * @return The Copycat execution context.
   */
  ExecutionContext executor();

  /**
   * Returns the Copycat log.
   *
   * @return The Copycat log.
   */
  Log log();

  /**
   * Registers an action.
   *
   * @param name The action to register.
   * @param action The action function.
   * @param <T> The action output type.
   * @return The Copycat context.
   */
  <T, U> CopycatContext register(String name, Action<T, U> action);

  /**
   * Registers an action.
   *
   * @param name The action to register.
   * @param action The action function.
   * @param options The action options.
   * @param <T> The action output type.
   * @return The Copycat context.
   */
  <T, U> CopycatContext register(String name, Action<T, U> action, ActionOptions options);

  /**
   * Unregisters an action.
   *
   * @param name The action to unregister.
   * @return The Copycat context.
   */
  CopycatContext unregister(String name);

  /**
   * Configures the context.
   *
   * @param config The cluster configuration.
   * @return A completable future to be completed once the context has been configured.
   */
  CompletableFuture<ClusterConfig> configure(ClusterConfig config);

  /**
   * Submits a named action to the context.
   *
   * @param action The action to submit.
   * @param entry The entry to submit.
   * @param <T> The output type.
   * @return A completable future to be completed once the action has been committed.
   */
  <T, U> CompletableFuture<U> submit(String action, T entry);

}
