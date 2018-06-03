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
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.utils.Services;

import java.util.Collection;
import java.util.Map;

/**
 * Primitive protocol type registry that scans the classpath for protocol implementations.
 */
public class ClasspathScanningPrimitiveProtocolTypeRegistry implements PrimitiveProtocolTypeRegistry {
  private final Map<String, PrimitiveProtocol.Type> protocolTypes = Maps.newConcurrentMap();

  public ClasspathScanningPrimitiveProtocolTypeRegistry(ClassLoader classLoader) {
    init(classLoader);
  }

  /**
   * Initializes the registry by scanning the classpath.
   */
  private void init(ClassLoader classLoader) {
    for (PrimitiveProtocol.Type protocolType : Services.loadTypes(PrimitiveProtocol.Type.class, classLoader)) {
      protocolTypes.put(protocolType.name(), protocolType);
    }
  }

  @Override
  public Collection<PrimitiveProtocol.Type> getProtocolTypes() {
    return protocolTypes.values();
  }

  @Override
  public PrimitiveProtocol.Type getProtocolType(String type) {
    return protocolTypes.get(type);
  }
}
