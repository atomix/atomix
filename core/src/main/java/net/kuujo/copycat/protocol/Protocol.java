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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.Managed;

import java.util.concurrent.CompletableFuture;

/**
 * Protocol instance.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Protocol extends Managed<Protocol> {

  /**
   * Returns the protocol cluster.
   *
   * @return The protocol cluster.
   */
  Cluster cluster();

  /**
   * Returns the protocol topic.
   *
   * @return The protocol topic.
   */
  String topic();

  /**
   * Returns the protocol serializer.
   *
   * @return The protocol serializer.
   */
  Serializer serializer();

  /**
   * Returns the protocol execution context.
   *
   * @return The protocol execution context.
   */
  ExecutionContext context();

  /**
   * Return the local protocol time.
   *
   * @return The local protocol time.
   */
  long localTime();

  /**
   * Returns the global protocol time.
   *
   * @return The global protocol time.
   */
  long globalTime();

  /**
   * Sets the protocol filter.
   *
   * @param filter The filter.
   */
  Protocol setFilter(ProtocolFilter filter);

  /**
   * Sets the protocol commit handler.
   *
   * @param handler The protocol commit handler.
   */
  Protocol setHandler(ProtocolHandler handler);

  /**
   * Submits a command to the protocol with the default persistence and consistency levels.
   *
   * @param command The command to submit.
   * @return A completable future to be completed with the command result.
   */
  default <R> CompletableFuture<R> submit(Object command) {
    return submit(command, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Submits a command to the protocol with the default consistency level.
   *
   * @param command The command to submit.
   * @param persistence The command persistence level.
   * @return A completable future to be completed with the command result.
   */
  default <R> CompletableFuture<R> submit(Object command, Persistence persistence) {
    return submit(command, persistence, Consistency.DEFAULT);
  }

  /**
   * Submits a command to the protocol with the default persistence level.
   *
   * @param command The command to submit.
   * @param consistency The command consistency requirement.
   * @return A completable future to be completed with the command result.
   */
  default <R> CompletableFuture<R> submit(Object command, Consistency consistency) {
    return submit(command, Persistence.DEFAULT, consistency);
  }

  /**
   * Submits a command to the protocol.
   *
   * @param command The command to submit.
   * @param persistence The command persistence level.
   * @param consistency The command consistency requirement.
   * @return A completable future to be completed with the command result.
   */
  <R> CompletableFuture<R> submit(Object command, Persistence persistence, Consistency consistency);

  /**
   * Deletes the protocol's storage.
   *
   * @return A completable future to be completed once the protocol is deleted.
   */
  CompletableFuture<Void> delete();

}
