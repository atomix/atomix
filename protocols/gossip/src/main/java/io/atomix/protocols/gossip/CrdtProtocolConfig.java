// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.gossip;

import com.google.common.annotations.Beta;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * CRDT protocol configuration.
 */
@Beta
public class CrdtProtocolConfig extends PrimitiveProtocolConfig<CrdtProtocolConfig> {
  private TimestampProvider timestampProvider = TimestampProviders.WALL_CLOCK;
  private Duration gossipInterval = Duration.ofMillis(50);

  @Override
  public PrimitiveProtocol.Type getType() {
    return AntiEntropyProtocol.TYPE;
  }

  /**
   * Returns the configured timestamp provider.
   *
   * @return the configured timestamp provider
   */
  public TimestampProvider getTimestampProvider() {
    return timestampProvider;
  }

  /**
   * Sets the timestamp provider.
   *
   * @param timestampProvider the timestamp provider
   * @return the CRDT protocol configuration
   */
  public CrdtProtocolConfig setTimestampProvider(TimestampProvider timestampProvider) {
    this.timestampProvider = checkNotNull(timestampProvider);
    return this;
  }

  /**
   * Sets the timestamp provider.
   *
   * @param timestampProvider the timestamp provider
   * @return the CRDT protocol configuration
   */
  public CrdtProtocolConfig setTimestampProvider(TimestampProviders timestampProvider) {
    return setTimestampProvider((TimestampProvider) timestampProvider);
  }

  /**
   * Returns the gossip interval.
   *
   * @return the gossip interval
   */
  public Duration getGossipInterval() {
    return gossipInterval;
  }

  /**
   * Sets the gossip interval.
   *
   * @param gossipInterval the gossip interval
   * @return the anti-entropy protocol configuration
   */
  public CrdtProtocolConfig setGossipInterval(Duration gossipInterval) {
    this.gossipInterval = checkNotNull(gossipInterval);
    return this;
  }
}
