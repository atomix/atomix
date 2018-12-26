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
