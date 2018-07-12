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
package io.atomix.protocols.gossip;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.map.MapDelegate;
import io.atomix.primitive.protocol.map.MapProtocol;
import io.atomix.primitive.protocol.set.SetDelegate;
import io.atomix.primitive.protocol.set.SetProtocol;
import io.atomix.protocols.gossip.map.AntiEntropyMap;
import io.atomix.protocols.gossip.set.AntiEntropySet;
import io.atomix.utils.serializer.Serializer;

/**
 * Anti-entropy protocol.
 */
public class AntiEntropyProtocol implements GossipProtocol, MapProtocol, SetProtocol {
  public static final Type TYPE = new Type();

  /**
   * Returns a new gossip protocol builder.
   *
   * @return a new gossip protocol builder
   */
  public static AntiEntropyProtocolBuilder builder() {
    return new AntiEntropyProtocolBuilder(new AntiEntropyProtocolConfig());
  }

  /**
   * Gossip protocol type.
   */
  public static final class Type implements PrimitiveProtocol.Type<AntiEntropyProtocolConfig> {
    private static final String NAME = "gossip";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public AntiEntropyProtocolConfig newConfig() {
      return new AntiEntropyProtocolConfig();
    }

    @Override
    public PrimitiveProtocol newProtocol(AntiEntropyProtocolConfig config) {
      return new AntiEntropyProtocol(config);
    }
  }

  protected final AntiEntropyProtocolConfig config;

  protected AntiEntropyProtocol(AntiEntropyProtocolConfig config) {
    this.config = config;
  }

  @Override
  public PrimitiveProtocol.Type type() {
    return TYPE;
  }

  @Override
  public Serializer serializer() {
    return config.getSerializer();
  }

  @Override
  public <K, V> MapDelegate<K, V> newMapDelegate(String name, PrimitiveManagementService managementService) {
    return new AntiEntropyMap<>(name, config, managementService);
  }

  @Override
  public <E> SetDelegate<E> newSetDelegate(String name, PrimitiveManagementService managementService) {
    return new AntiEntropySet<>(name, config, managementService);
  }
}
