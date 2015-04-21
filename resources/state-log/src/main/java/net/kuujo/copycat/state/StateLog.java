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
package net.kuujo.copycat.state;

import net.kuujo.copycat.resource.Resource;

import java.util.concurrent.CompletableFuture;

/**
 * State log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateLog<K, V> extends Resource<StateLog<K, V>> {

  /**
   * Submits a state command or query to the log.
   *
   * @param command The command name.
   * @param entry The command entry.
   * @param <U> The command return type.
   * @return A completable future to be completed once the command output is received.
   */
  default <U> CompletableFuture<U> submit(String command, V entry) {
    return submit(command, null, entry);
  }

  /**
   * Submits a state command or query to the log.
   *
   * @param command The command name.
   * @param key The entry key.
   * @param entry The command entry.
   * @param <U> The command return type.
   * @return A completable future to be completed once the command output is received.
   */
  <U> CompletableFuture<U> submit(String command, K key, V entry);

}
