// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.impl;

import io.atomix.primitive.serialization.SerializationService;
import io.atomix.utils.serializer.SerializerBuilder;

/**
 * Core serialization service.
 */
public class CoreSerializationService implements SerializationService {
  private final boolean registrationRequired;
  private final boolean compatibleSerialization;

  public CoreSerializationService(boolean registrationRequired, boolean compatibleSerialization) {
    this.registrationRequired = registrationRequired;
    this.compatibleSerialization = compatibleSerialization;
  }

  @Override
  public SerializerBuilder newBuilder() {
    return new SerializerBuilder()
        .withRegistrationRequired(registrationRequired)
        .withCompatibleSerialization(compatibleSerialization);
  }

  @Override
  public SerializerBuilder newBuilder(String name) {
    return new SerializerBuilder(name)
        .withRegistrationRequired(registrationRequired)
        .withCompatibleSerialization(compatibleSerialization);
  }
}
