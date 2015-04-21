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
import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.resource.Resource;

import java.util.concurrent.CompletableFuture;

/**
 * Copycat protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Protocol {

  /**
   * Registers a protocol handler.
   *
   * @param handler The protocol handler.
   * @return The protocol.
   */
  Protocol handler(ProtocolHandler handler);

  /**
   * Executes a read.
   *
   * @param key The key to read.
   * @param entry The read arguments.
   * @param consistency The read consistency.
   * @return The asynchronous read result.
   */
  CompletableFuture<Buffer> read(Buffer key, Buffer entry, Consistency consistency);

  /**
   * Executes a write.
   *
   * @param key The key to write.
   * @param entry The write arguments.
   * @param consistency The write consistency.
   * @return The asynchronous write result.
   */
  CompletableFuture<Buffer> write(Buffer key, Buffer entry, Consistency consistency);

  /**
   * Executes a delete.
   *
   * @param key The key to delete.
   * @param entry The delete arguments.
   * @param consistency The delete consistency.
   * @return The asynchronous delete result.
   */
  CompletableFuture<Buffer> delete(Buffer key, Buffer entry, Consistency consistency);

  /**
   * Opens the protocol.
   *
   * @param resource The resource.
   * @return A completable future to be called once the protocol is opened.
   */
  CompletableFuture<Void> open(Resource resource);

  /**
   * Closes the protocol.
   *
   * @return A completable future to be called once the protocol is closed.
   */
  CompletableFuture<Void> close();

}
