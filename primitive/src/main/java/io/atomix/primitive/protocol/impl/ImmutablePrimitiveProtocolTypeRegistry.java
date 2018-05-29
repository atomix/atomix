/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitive.protocol.impl;

import com.google.common.collect.ImmutableList;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;
import io.atomix.primitive.protocol.PrimitiveProtocolType;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;

import java.util.Collection;

/**
 * Immutable primitive protocol type registry.
 */
public class ImmutablePrimitiveProtocolTypeRegistry implements PrimitiveProtocolTypeRegistry {
  private final PrimitiveProtocolTypeRegistry protocolTypes;

  public ImmutablePrimitiveProtocolTypeRegistry(PrimitiveProtocolTypeRegistry protocolTypes) {
    this.protocolTypes = protocolTypes;
  }

  @Override
  public Collection<PrimitiveProtocolType> getProtocolTypes() {
    return ImmutableList.copyOf(protocolTypes.getProtocolTypes());
  }

  @Override
  public PrimitiveProtocolType getProtocolType(String name) {
    return protocolTypes.getProtocolType(name);
  }

  @Override
  public PrimitiveProtocol createProtocol(PrimitiveProtocolConfig config) {
    return protocolTypes.createProtocol(config);
  }
}
