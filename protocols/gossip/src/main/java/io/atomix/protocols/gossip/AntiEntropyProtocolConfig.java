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
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Anti-entropy protocol configuration.
 */
@Beta
public class AntiEntropyProtocolConfig extends PrimitiveProtocolConfig<AntiEntropyProtocolConfig> {
  private TimestampProvider timestampProvider = TimestampProviders.WALL_CLOCK;
  private Set<String> peers;
  private PeerSelector peerSelector = PeerSelectors.RANDOM;
  private boolean tombstonesDisabled;
  private Duration gossipInterval = Duration.ofMillis(50);
  private Duration antiEntropyInterval = Duration.ofMillis(500);

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
   * @return the anti-entropy protocol configuration
   */
  public AntiEntropyProtocolConfig setTimestampProvider(TimestampProvider timestampProvider) {
    this.timestampProvider = checkNotNull(timestampProvider);
    return this;
  }

  /**
   * Sets the timestamp provider.
   *
   * @param timestampProvider the timestamp provider
   * @return the anti-entropy protocol configuration
   */
  public AntiEntropyProtocolConfig setTimestampProvider(TimestampProviders timestampProvider) {
    return setTimestampProvider((TimestampProvider) timestampProvider);
  }

  /**
   * Returns the set of peers with which to gossip.
   *
   * @return the set of peers with which to gossip
   */
  public Set<String> getPeers() {
    return peers;
  }

  /**
   * Sets the set of peers with which to gossip.
   *
   * @param peers the set of peers with which to gossip
   * @return the anti-entropy protocol configuration
   */
  public AntiEntropyProtocolConfig setPeers(Set<String> peers) {
    this.peers = peers;
    return this;
  }

  /**
   * Returns the gossip peer selector.
   *
   * @return the gossip peer selector
   */
  public PeerSelector getPeerSelector() {
    return peerSelector;
  }

  /**
   * Sets the gossip peer selector.
   *
   * @param peerSelector the gossip peer selector
   * @return the anti-entropy protocol configuration
   */
  public AntiEntropyProtocolConfig setPeerSelector(PeerSelector peerSelector) {
    this.peerSelector = checkNotNull(peerSelector);
    return this;
  }

  /**
   * Sets the gossip peer selector.
   *
   * @param peerSelector the gossip peer selector
   * @return the anti-entropy protocol configuration
   */
  public AntiEntropyProtocolConfig setPeerSelector(PeerSelectors peerSelector) {
    return setPeerSelector((PeerSelector) peerSelector);
  }

  /**
   * Returns whether tombstones are enabled.
   *
   * @return whether tombstones are enabled
   */
  public boolean isTombstonesDisabled() {
    return tombstonesDisabled;
  }

  /**
   * Sets whether tombstones are enabled.
   *
   * @param tombstonesDisabled whether tombstones are enabled
   * @return the anti-entropy protocol configuration
   */
  public AntiEntropyProtocolConfig setTombstonesDisabled(boolean tombstonesDisabled) {
    this.tombstonesDisabled = tombstonesDisabled;
    return this;
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
  public AntiEntropyProtocolConfig setGossipInterval(Duration gossipInterval) {
    this.gossipInterval = checkNotNull(gossipInterval);
    return this;
  }

  /**
   * Returns the anti-entropy advertisement interval.
   *
   * @return the anti-entropy advertisement interval
   */
  public Duration getAntiEntropyInterval() {
    return antiEntropyInterval;
  }

  /**
   * Sets the anti-entropy advertisement interval.
   *
   * @param antiEntropyInterval the anti-entropy advertisement interval
   * @return the anti-entropy protocol configuration
   */
  public AntiEntropyProtocolConfig setAntiEntropyInterval(Duration antiEntropyInterval) {
    this.antiEntropyInterval = checkNotNull(antiEntropyInterval);
    return this;
  }
}
