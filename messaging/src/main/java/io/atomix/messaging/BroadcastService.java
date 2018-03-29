/*
 * Copyright 2018-present Open Networking Foundation
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

import io.atomix.utils.Builder;

import java.util.function.Consumer;

/**
 * Broadcast service.
 */
public interface BroadcastService {

  /**
   * Broadcasts the given message.
   *
   * @param message the message to broadcast
   */
  void broadcast(byte[] message);

  /**
   * Adds a broadcast listener.
   *
   * @param listener the broadcast listener to add
   */
  void addListener(Consumer<byte[]> listener);

  /**
   * Removes a broadcast listener.
   *
   * @param listener the broadcast listener to remove
   */
  void removeListener(Consumer<byte[]> listener);

  /**
   * Broadcast service builder.
   */
  interface Builder extends io.atomix.utils.Builder<BroadcastService> {
  }
}
