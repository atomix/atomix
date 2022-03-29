// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.set;

import com.google.common.annotations.Beta;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.utils.serializer.Serializer;

/**
 * Sorted set protocol.
 */
@Beta
public interface SortedSetProtocol extends SetProtocol {

  /**
   * Returns a new set delegate.
   *
   * @param name the set name
   * @param serializer the set element serializer
   * @param managementService the primitive management service
   * @param <E> the set element type
   * @return a new set delegate
   */
  <E> SortedSetDelegate<E> newSortedSetDelegate(String name, Serializer serializer, PrimitiveManagementService managementService);

}
