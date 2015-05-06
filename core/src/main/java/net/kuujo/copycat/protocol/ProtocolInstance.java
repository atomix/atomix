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

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.util.Managed;

import java.util.concurrent.CompletableFuture;

/**
 * Protocol instance.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ProtocolInstance extends Managed<ProtocolInstance> {

  /**
   * Sets the protocol filter.
   *
   * @param filter The filter.
   */
  ProtocolInstance setFilter(ProtocolFilter filter);

  /**
   * Sets the protocol commit handler.
   *
   * @param handler The protocol commit handler.
   */
  ProtocolInstance setHandler(ProtocolHandler handler);

  /**
   * Submits a keyless command to the protocol.
   *
   * @param entry The command entry.
   * @return A completable future to be completed with the command result.
   */
  default CompletableFuture<Buffer> submit(Buffer entry) {
    return submit(null, entry, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Submits a keyless command to the protocol with the default consistency level.
   *
   * @param entry The command entry.
   * @param persistence The command persistence level.
   * @return A completable future to be completed with the command result.
   */
  default CompletableFuture<Buffer> submit(Buffer entry, Persistence persistence) {
    return submit(null, entry, persistence, Consistency.DEFAULT);
  }

  /**
   * Submits a keyless command to the protocol with the default persistence level.
   *
   * @param entry The command entry.
   * @param consistency The command consistency requirement.
   * @return A completable future to be completed with the command result.
   */
  default CompletableFuture<Buffer> submit(Buffer entry, Consistency consistency) {
    return submit(null, entry, Persistence.DEFAULT, consistency);
  }

  /**
   * Submits a command to the protocol with the default persistence and consistency levels.
   *
   * @param key The command key.
   * @param entry The command entry.
   * @return A completable future to be completed with the command result.
   */
  default CompletableFuture<Buffer> submit(Buffer key, Buffer entry) {
    return submit(key, entry, Persistence.DEFAULT, Consistency.DEFAULT);
  }

  /**
   * Submits a command to the protocol with the default consistency level.
   *
   * @param key The command key.
   * @param entry The command entry.
   * @param persistence The command persistence level.
   * @return A completable future to be completed with the command result.
   */
  default CompletableFuture<Buffer> submit(Buffer key, Buffer entry, Persistence persistence) {
    return submit(key, entry, persistence, Consistency.DEFAULT);
  }

  /**
   * Submits a command to the protocol with the default persistence level.
   *
   * @param key The command key.
   * @param entry The command entry.
   * @param consistency The command consistency requirement.
   * @return A completable future to be completed with the command result.
   */
  default CompletableFuture<Buffer> submit(Buffer key, Buffer entry, Consistency consistency) {
    return submit(key, entry, Persistence.DEFAULT, consistency);
  }

  /**
   * Submits a command to the protocol.
   *
   * @param key The command key.
   * @param entry The command entry.
   * @param persistence The command persistence level.
   * @param consistency The command consistency requirement.
   * @return A completable future to be completed with the command result.
   */
  CompletableFuture<Buffer> submit(Buffer key, Buffer entry, Persistence persistence, Consistency consistency);

}
