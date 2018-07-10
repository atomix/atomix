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
import io.atomix.primitive.protocol.counter.CounterProtocol;
import io.atomix.protocols.gossip.counter.GossipCounter;
import io.atomix.utils.serializer.Serializer;

/**
 * Dissemination protocol.
 */
public class DisseminationProtocol implements GossipProtocol {
  public static final Type TYPE = new Type();

  /**
   * Returns a new gossip protocol builder.
   *
   * @return a new gossip protocol builder
   */
  public static DisseminationProtocolBuilder builder() {
    return new DisseminationProtocolBuilder(new DisseminationProtocolConfig());
  }

  /**
   * Gossip protocol type.
   */
  public static final class Type implements PrimitiveProtocol.Type<DisseminationProtocolConfig> {
    private static final String NAME = "gossip";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public DisseminationProtocolConfig newConfig() {
      return new DisseminationProtocolConfig();
    }

    @Override
    public PrimitiveProtocol newProtocol(DisseminationProtocolConfig config) {
      return new DisseminationProtocol(config);
    }
  }

  protected final DisseminationProtocolConfig config;

  protected DisseminationProtocol(DisseminationProtocolConfig config) {
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
  public CounterProtocol newCounterProtocol(String name, PrimitiveManagementService managementService) {
    return new GossipCounter(name, managementService);
  }
}
