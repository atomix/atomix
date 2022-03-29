// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.map;

/**
 * Map builder.
 */
public interface SortedMapCompatibleBuilder<B extends SortedMapCompatibleBuilder<B>> {

  /**
   * Configures the builder with a map compatible gossip protocol.
   *
   * @param protocol the map protocol
   * @return the primitive builder
   */
  B withProtocol(SortedMapProtocol protocol);

}
