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
import io.atomix.primitive.protocol.PrimitiveProtocolBuilder;

import java.time.Duration;
import java.util.Set;

/**
 * Anti-entropy protocol builder.
 */
@Beta
public class AntiEntropyProtocolBuilder extends PrimitiveProtocolBuilder<AntiEntropyProtocolBuilder, AntiEntropyProtocolConfig, AntiEntropyProtocol> {
  public AntiEntropyProtocolBuilder(AntiEntropyProtocolConfig config) {
    super(config);
  }

  /**
   * Sets the timestamp provider.
   *
   * @param timestampProvider the timestamp provider
   * @return the anti-entropy protocol configuration
   */
  public <E> AntiEntropyProtocolBuilder withTimestampProvider(TimestampProvider<E> timestampProvider) {
    config.setTimestampProvider(timestampProvider);
    return this;
  }

  /**
   * Sets the set of peers with which to gossip.
   *
   * @param peers the set of peers with which to gossip
   * @return the anti-entropy protocol configuration
   */
  public AntiEntropyProtocolBuilder withPeers(Set<String> peers) {
    config.setPeers(peers);
    return this;
  }

  /**
   * Sets the gossip peer selector.
   *
   * @param peerSelector the gossip peer selector
   * @return the anti-entropy protocol configuration
   */
  public <E> AntiEntropyProtocolBuilder withPeerSelector(PeerSelector<E> peerSelector) {
    config.setPeerSelector(peerSelector);
    return this;
  }

  /**
   * Sets whether tombstones are enabled.
   *
   * @param tombstonesDisabled whether tombstones are enabled
   * @return the anti-entropy protocol configuration
   */
  public AntiEntropyProtocolBuilder withTombstonesDisabled(boolean tombstonesDisabled) {
    config.setTombstonesDisabled(tombstonesDisabled);
    return this;
  }

  /**
   * Sets the gossip interval.
   *
   * @param gossipInterval the gossip interval
   * @return the anti-entropy protocol configuration
   */
  public AntiEntropyProtocolBuilder withGossipInterval(Duration gossipInterval) {
    config.setGossipInterval(gossipInterval);
    return this;
  }

  /**
   * Sets the anti-entropy advertisement interval.
   *
   * @param antiEntropyInterval the anti-entropy advertisement interval
   * @return the anti-entropy protocol configuration
   */
  public AntiEntropyProtocolBuilder withAntiEntropyInterval(Duration antiEntropyInterval) {
    config.setAntiEntropyInterval(antiEntropyInterval);
    return this;
  }

  @Override
  public AntiEntropyProtocol build() {
    return new AntiEntropyProtocol(config);
  }
}
