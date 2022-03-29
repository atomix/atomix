// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip;

import com.google.common.annotations.Beta;
import io.atomix.primitive.protocol.PrimitiveProtocolBuilder;

import java.time.Duration;

/**
 * CRDT protocol builder.
 */
@Beta
public class CrdtProtocolBuilder extends PrimitiveProtocolBuilder<CrdtProtocolBuilder, CrdtProtocolConfig, CrdtProtocol> {
  public CrdtProtocolBuilder(CrdtProtocolConfig config) {
    super(config);
  }

  /**
   * Sets the timestamp provider.
   *
   * @param timestampProvider the timestamp provider
   * @return the CRDT protocol builder
   */
  public CrdtProtocolBuilder withTimestampProvider(TimestampProvider timestampProvider) {
    config.setTimestampProvider(timestampProvider);
    return this;
  }

  /**
   * Sets the gossip interval.
   *
   * @param gossipInterval the gossip interval
   * @return the CRDT protocol builder
   */
  public CrdtProtocolBuilder withGossipInterval(Duration gossipInterval) {
    config.setGossipInterval(gossipInterval);
    return this;
  }

  @Override
  public CrdtProtocol build() {
    return new CrdtProtocol(config);
  }
}
