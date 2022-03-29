// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.serialization;

import io.atomix.utils.serializer.SerializerBuilder;

/**
 * Primitive serialization service.
 */
public interface SerializationService {

  /**
   * Returns a new serializer builder.
   *
   * @return a new serializer builder
   */
  SerializerBuilder newBuilder();

  /**
   * Returns a new serializer builder.
   *
   * @param name the serializer builder name
   * @return the serializer builder
   */
  SerializerBuilder newBuilder(String name);

}
