// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Messaging handler registry.
 */
final class HandlerRegistry {
  private final Map<String, BiConsumer<ProtocolRequest, ServerConnection>> handlers = new ConcurrentHashMap<>();

  /**
   * Registers a message type handler.
   *
   * @param type    the message type
   * @param handler the message handler
   */
  void register(String type, BiConsumer<ProtocolRequest, ServerConnection> handler) {
    handlers.put(type, handler);
  }

  /**
   * Unregisters a message type handler.
   *
   * @param type the message type
   */
  void unregister(String type) {
    handlers.remove(type);
  }

  /**
   * Looks up a message type handler.
   *
   * @param type the message type
   * @return the message handler or {@code null} if no handler of the given type is registered
   */
  BiConsumer<ProtocolRequest, ServerConnection> get(String type) {
    return handlers.get(type);
  }
}
