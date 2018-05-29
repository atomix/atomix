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

import com.google.common.collect.Maps;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;
import io.atomix.primitive.protocol.PrimitiveProtocolType;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.utils.config.ConfigurationException;

import java.util.Collection;
import java.util.Map;

/**
 * Default primitive protocol type registry.
 */
public class DefaultPrimitiveProtocolTypeRegistry implements PrimitiveProtocolTypeRegistry {
  private final Map<String, PrimitiveProtocolType> protocolTypes = Maps.newConcurrentMap();

  public DefaultPrimitiveProtocolTypeRegistry(Collection<PrimitiveProtocolType> protocolTypes) {
    protocolTypes.forEach(protocolType -> this.protocolTypes.put(protocolType.name(), protocolType));
  }

  @Override
  public Collection<PrimitiveProtocolType> getProtocolTypes() {
    return protocolTypes.values();
  }

  @Override
  public PrimitiveProtocolType getProtocolType(String type) {
    return protocolTypes.get(type);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimitiveProtocol createProtocol(PrimitiveProtocolConfig config) {
    PrimitiveProtocolType protocolType = protocolTypes.get(config.getType());
    if (protocolType == null) {
      throw new ConfigurationException("Unknown protocol type " + config.getType());
    }
    return protocolType.createProtocol(config);
  }
}
