// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
