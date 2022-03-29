// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.map;

import com.google.common.annotations.Beta;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.utils.serializer.Serializer;

/**
 * Map protocol.
 */
@Beta
public interface MapProtocol extends GossipProtocol {

  /**
   * Returns a new map delegate.
   *
   * @param name the map name
   * @param serializer the map entry serializer
   * @param managementService the primitive management service
   * @return a new map delegate
   */
  <K, V> MapDelegate<K, V> newMapDelegate(String name, Serializer serializer, PrimitiveManagementService managementService);

}
