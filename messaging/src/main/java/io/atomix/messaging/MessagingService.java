/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.messaging;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Interface for low level messaging primitives.
 */
public interface MessagingService {

  /**
   * Returns the local messaging service endpoint.
   *
   * @return the local endpoint
   */
  Endpoint endpoint();

  /**
   * Sends a message asynchronously to the specified communication end point.
   * The message is specified using the type and payload.
   *
   * @param ep      end point to send the message to.
   * @param type    type of message.
   * @param payload message payload bytes.
   * @return future that is completed when the message is sent
   */
  CompletableFuture<Void> sendAsync(Endpoint ep, String type, byte[] payload);

  /**
   * Sends a message asynchronously and expects a response.
   *
   * @param ep      end point to send the message to.
   * @param type    type of message.
   * @param payload message payload.
   * @return a response future
   */
  CompletableFuture<byte[]> sendAndReceive(Endpoint ep, String type, byte[] payload);

  /**
   * Sends a message synchronously and expects a response.
   *
   * @param ep       end point to send the message to.
   * @param type     type of message.
   * @param payload  message payload.
   * @param executor executor over which any follow up actions after completion will be executed.
   * @return a response future
   */
  CompletableFuture<byte[]> sendAndReceive(Endpoint ep, String type, byte[] payload, Executor executor);

  /**
   * Registers a new message handler for message type.
   *
   * @param type     message type.
   * @param handler  message handler
   * @param executor executor to use for running message handler logic.
   */
  void registerHandler(String type, BiConsumer<Endpoint, byte[]> handler, Executor executor);

  /**
   * Registers a new message handler for message type.
   *
   * @param type     message type.
   * @param handler  message handler
   * @param executor executor to use for running message handler logic.
   */
  void registerHandler(String type, BiFunction<Endpoint, byte[], byte[]> handler, Executor executor);

  /**
   * Registers a new message handler for message type.
   *
   * @param type    message type.
   * @param handler message handler
   */
  void registerHandler(String type, BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> handler);

  /**
   * Unregister current handler, if one exists for message type.
   *
   * @param type message type
   */
  void unregisterHandler(String type);

  /**
   * Messaging service builder.
   */
  abstract class Builder implements io.atomix.utils.Builder<MessagingService> {
  }
}
