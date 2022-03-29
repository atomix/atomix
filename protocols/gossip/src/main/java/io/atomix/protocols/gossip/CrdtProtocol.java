// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip;

import com.google.common.annotations.Beta;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.counter.CounterDelegate;
import io.atomix.primitive.protocol.counter.CounterProtocol;
import io.atomix.primitive.protocol.set.NavigableSetDelegate;
import io.atomix.primitive.protocol.set.NavigableSetProtocol;
import io.atomix.primitive.protocol.set.SetDelegate;
import io.atomix.primitive.protocol.set.SortedSetDelegate;
import io.atomix.primitive.protocol.value.ValueDelegate;
import io.atomix.primitive.protocol.value.ValueProtocol;
import io.atomix.protocols.gossip.counter.CrdtCounterDelegate;
import io.atomix.protocols.gossip.set.CrdtNavigableSetDelegate;
import io.atomix.protocols.gossip.set.CrdtSetDelegate;
import io.atomix.protocols.gossip.value.CrdtValueDelegate;
import io.atomix.utils.serializer.Serializer;

/**
 * Conflict-free Replicated Data Types (CRDT) protocol.
 */
@Beta
public class CrdtProtocol implements GossipProtocol, CounterProtocol, NavigableSetProtocol, ValueProtocol {
  public static final Type TYPE = new Type();

  /**
   * Returns an instance of the CRDT protocol with the default configuration.
   *
   * @return an instance of the CRDT protocol with the default configuration
   */
  public static CrdtProtocol instance() {
    return new CrdtProtocol(new CrdtProtocolConfig());
  }

  /**
   * Returns a new CRDT protocol builder.
   *
   * @return a new CRDT protocol builder
   */
  public static CrdtProtocolBuilder builder() {
    return new CrdtProtocolBuilder(new CrdtProtocolConfig());
  }

  /**
   * CRDT protocol type.
   */
  public static final class Type implements PrimitiveProtocol.Type<CrdtProtocolConfig> {
    private static final String NAME = "crdt";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public CrdtProtocolConfig newConfig() {
      return new CrdtProtocolConfig();
    }

    @Override
    public PrimitiveProtocol newProtocol(CrdtProtocolConfig config) {
      return new CrdtProtocol(config);
    }
  }

  protected final CrdtProtocolConfig config;

  protected CrdtProtocol(CrdtProtocolConfig config) {
    this.config = config;
  }

  @Override
  public PrimitiveProtocol.Type type() {
    return TYPE;
  }

  @Override
  public CounterDelegate newCounterDelegate(String name, PrimitiveManagementService managementService) {
    return new CrdtCounterDelegate(name, config, managementService);
  }

  @Override
  public <E> SetDelegate<E> newSetDelegate(String name, Serializer serializer, PrimitiveManagementService managementService) {
    return new CrdtSetDelegate<>(name, serializer, config, managementService);
  }

  @Override
  public <E> SortedSetDelegate<E> newSortedSetDelegate(String name, Serializer serializer, PrimitiveManagementService managementService) {
    return new CrdtNavigableSetDelegate<>(name, serializer, config, managementService);
  }

  @Override
  public <E> NavigableSetDelegate<E> newNavigableSetDelegate(String name, Serializer serializer, PrimitiveManagementService managementService) {
    return new CrdtNavigableSetDelegate<>(name, serializer, config, managementService);
  }

  @Override
  public ValueDelegate newValueDelegate(String name, Serializer serializer, PrimitiveManagementService managementService) {
    return new CrdtValueDelegate(name, serializer, config, managementService);
  }
}
