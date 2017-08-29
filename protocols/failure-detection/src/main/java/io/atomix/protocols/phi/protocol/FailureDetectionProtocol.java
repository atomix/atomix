/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.phi.protocol;

import io.atomix.utils.Identifier;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Failure detection protocol.
 */
public interface FailureDetectionProtocol<T extends Identifier> {

  /**
   * Sends the given heartbeat message to the given [eer.
   *
   * @param peer the identifier of the peer to which to send the heartbeat
   * @param message the heartbeat message to send
   * @return a future to be completed once the heartbeat has been sent
   */
  CompletableFuture<Void> heartbeat(T peer, HeartbeatMessage<T> message);

  /**
   * Registers a heartbeat message listener.
   *
   * @param listener the heartbeat message listener
   */
  void registerHeartbeatListener(Consumer<HeartbeatMessage<T>> listener);

  /**
   * Unregisters the heartbeat message listener.
   */
  void unregisterHeartbeatListener();

}
