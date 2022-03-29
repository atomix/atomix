// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging;

import java.util.function.Consumer;

/**
 * Service for broadcast messaging between nodes.
 * <p>
 * The broadcast service is an unreliable broadcast messaging service backed by multicast. This service provides no
 * guaranteed regarding reliability or order of messages.
 */
public interface BroadcastService {

  /**
   * Broadcasts the given message to all listeners for the given subject.
   * <p>
   * The message will be broadcast to all listeners for the given {@code subject}. This service makes no guarantee
   * regarding the reliability or order of delivery of the message.
   *
   * @param subject the message subject
   * @param message the message to broadcast
   */
  void broadcast(String subject, byte[] message);

  /**
   * Adds a broadcast listener for the given subject.
   * <p>
   * Messages broadcast to the given {@code subject} will be delivered to the provided listener. This service provides
   * no guarantee regarding the order in which messages arrive.
   *
   * @param subject the message subject
   * @param listener the broadcast listener to add
   */
  void addListener(String subject, Consumer<byte[]> listener);

  /**
   * Removes a broadcast listener for the given subject.
   *
   * @param subject the message subject
   * @param listener the broadcast listener to remove
   */
  void removeListener(String subject, Consumer<byte[]> listener);

  /**
   * Broadcast service builder.
   */
  interface Builder extends io.atomix.utils.Builder<BroadcastService> {
  }
}
