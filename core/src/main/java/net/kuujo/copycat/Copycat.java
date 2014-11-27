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

import java.util.concurrent.CompletableFuture;

/**
 * Copycat replicated state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Copycat extends Configurable<Copycat, CopycatConfig>, Managed {

  /**
   * Returns a new Copycat instance.
   *
   * @return A new Copycat instance.
   */
  static Copycat copycat() {
    return null;
  }

  /**
   * Returns a new Copycat instance.
   *
   * @param config The Copycat configuration.
   * @return A new Copycat instance.
   */
  static Copycat copycat(CopycatConfig config) {
    return null;
  }

  /**
   * Returns the Copycat cluster.
   *
   * @return The Copycat cluster.
   */
  Cluster cluster();

  /**
   * Returns the underlying Copycat configuration.<p>
   *
   * Note that altering the underlying configuration will not alter the configuration of the Copycat instance.
   * Configuration changes must be explicitly committed to the underlying log via the {@link #configure(Object)} method.
   *
   * @return The underlying Copycat configuration.
   */
  CopycatConfig config();

  /**
   * Submits an operation to the cluster.
   *
   * @param operation The operation to execute.
   * @param args The operation arguments.
   * @param <T> The operation result type.
   * @return A completable future to be completed with the operation result.
   */
  <T> CompletableFuture<T> submit(String operation, Object... args);

}
