// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.map;

import com.google.common.annotations.Beta;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.utils.serializer.Serializer;

/**
 * Navigable map protocol.
 */
@Beta
public interface NavigableMapProtocol extends SortedMapProtocol {

  /**
   * Returns a new navigable map delegate.
   *
   * @param name the map name
   * @param serializer the map entry serializer
   * @param managementService the primitive management service
   * @return a new map delegate
   */
  <K, V> NavigableMapDelegate<K, V> newNavigableMapDelegate(String name, Serializer serializer, PrimitiveManagementService managementService);

}
