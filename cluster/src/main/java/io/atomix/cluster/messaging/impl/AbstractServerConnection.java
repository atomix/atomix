/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.messaging.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * Base class for server-side connections. Manages dispatching requests to message handlers.
 */
abstract class AbstractServerConnection implements ServerConnection {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final HandlerRegistry handlers;

  AbstractServerConnection(HandlerRegistry handlers) {
    this.handlers = handlers;
  }

  @Override
  public void dispatch(ProtocolRequest message) {
    BiConsumer<ProtocolRequest, ServerConnection> handler = handlers.get(message.subject());
    if (handler != null) {
      log.trace("Received message type {} from {}", message.subject(), message.sender());
      handler.accept(message, this);
    } else {
      log.debug("No handler for message type {} from {}", message.subject(), message.sender());
      reply(message, ProtocolReply.Status.ERROR_NO_HANDLER, Optional.empty());
    }
  }
}