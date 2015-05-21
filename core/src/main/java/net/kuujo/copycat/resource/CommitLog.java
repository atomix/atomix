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
package net.kuujo.copycat.resource;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.util.Managed;

import java.util.concurrent.CompletableFuture;

/**
 * Commit log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CommitLog extends Managed<CommitLog> {

  /**
   * Returns the commit log cluster.
   *
   * @return The commit log cluster.
   */
  Cluster cluster();

  /**
   * Submits a command to the commit log.
   *
   * @param command The command to submit.
   * @param <R> The command output type.
   * @return A completable future to be completed with the command result.
   */
  <R> CompletableFuture<R> submit(Command<R> command);

  /**
   * Submits a command to the log.
   *
   * @param type The command type.
   * @param processor The command processor.
   * @return A completable future to be completed with the command result.
   */
  <T extends Command<R>, R> CompletableFuture<R> submit(CommandType<T> type, CommandProcessor<T> processor);

  /**
   * Submits a command to the log.
   *
   * @param type The command type.
   * @param processor The command processor.
   * @return A completable future to be completed with the command result.
   */
  <T extends Command<R>, R> CompletableFuture<R> submit(Class<T> type, CommandProcessor<T> processor);

}
