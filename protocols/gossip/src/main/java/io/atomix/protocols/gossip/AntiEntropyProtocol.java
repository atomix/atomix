// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip;

import com.google.common.annotations.Beta;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.map.MapDelegate;
import io.atomix.primitive.protocol.map.MapProtocol;
import io.atomix.primitive.protocol.set.SetDelegate;
import io.atomix.primitive.protocol.set.SetProtocol;
import io.atomix.protocols.gossip.map.AntiEntropyMapDelegate;
import io.atomix.protocols.gossip.set.AntiEntropySetDelegate;
import io.atomix.utils.serializer.Serializer;

/**
 * Anti-entropy protocol.
 */
@Beta
public class AntiEntropyProtocol implements GossipProtocol, MapProtocol, SetProtocol {
  public static final Type TYPE = new Type();

  /**
   * Returns an instance of the anti-entropy protocol with the default configuration.
   *
   * @return an instance of the anti-entropy protocol with the default configuration
   */
  public static AntiEntropyProtocol instance() {
    return new AntiEntropyProtocol(new AntiEntropyProtocolConfig());
  }

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
  public <K, V> MapDelegate<K, V> newMapDelegate(String name, Serializer serializer, PrimitiveManagementService managementService) {
    return new AntiEntropyMapDelegate<>(name, serializer, config, managementService);
  }

  @Override
  public <E> SetDelegate<E> newSetDelegate(String name, Serializer serializer, PrimitiveManagementService managementService) {
    return new AntiEntropySetDelegate<>(name, serializer, config, managementService);
  }
}
