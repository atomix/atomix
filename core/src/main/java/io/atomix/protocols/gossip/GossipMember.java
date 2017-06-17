/*
 * Copyright 2017-present Open Networking Laboratory
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

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.cluster.NodeId;
import io.atomix.event.EventSink;
import io.atomix.event.ListenerService;
import io.atomix.protocols.gossip.impl.DefaultGossipMember;
import io.atomix.protocols.gossip.protocol.GossipProtocol;
import io.atomix.time.Timestamp;
import io.atomix.time.WallClockTimestamp;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Gossip protocol.
 */
public interface GossipMember<K, V> extends ListenerService<GossipEvent<K, V>, GossipEventListener<K, V>>, EventSink<GossipEvent<K, V>> {

  /**
   * Returns a new gossip member builder.
   *
   * @param <K> the gossip subject type
   * @param <V> the gossip value type
   * @return a new gossip member builder
   */
  static <K, V> Builder<K, V> builder() {
    return new DefaultGossipMember.Builder<>();
  }

  /**
   * Closes the member.
   */
  void close();

  /**
   * Gossip protocol builder.
   *
   * @param <K> the gossip subject type
   * @param <V> the gossip value type
   */
  abstract class Builder<K, V> implements io.atomix.util.Builder<GossipMember<K, V>> {
    protected GossipProtocol protocol;
    protected Supplier<Collection<NodeId>> peerProvider;
    protected Executor eventExecutor = MoreExecutors.directExecutor();
    protected ScheduledExecutorService communicationExecutor;
    protected Duration updateInterval = Duration.ofSeconds(1);
    protected boolean fastConvergence = false;
    protected boolean tombstonesDisabled = false;
    protected Duration purgeInterval = Duration.ofMinutes(1);

    /**
     * Sets the gossip protocol.
     *
     * @param protocol the gossip protocol
     * @return the gossip member builder
     * @throws NullPointerException if the protocol is null
     */
    public Builder<K, V> withProtocol(GossipProtocol protocol) {
      this.protocol = checkNotNull(protocol, "protocol");
      return this;
    }

    /**
     * Sets the gossip peer provider function.
     *
     * @param peerProvider the gossip peer provider
     * @return the gossip member builder
     * @throws NullPointerException if the peer provider is null
     */
    public Builder<K, V> withPeerProvider(Supplier<Collection<NodeId>> peerProvider) {
      this.peerProvider = checkNotNull(peerProvider, "peerProvider cannot be null");
      return this;
    }

    /**
     * Sets the gossip event executor.
     *
     * @param executor the gossip event executor
     * @return the gossip member builder
     * @throws NullPointerException if the event executor is null
     */
    public Builder<K, V> withEventExecutor(Executor executor) {
      this.eventExecutor = checkNotNull(executor, "executor cannot be null");
      return this;
    }

    /**
     * Sets the gossip communication executor.
     *
     * @param executor the gossip communication executor
     * @return the gossip member builder
     * @throws NullPointerException if the communication executor is null
     */
    public Builder<K, V> withCommunicationExecutor(ScheduledExecutorService executor) {
      this.communicationExecutor = checkNotNull(executor, "executor cannot be null");
      return this;
    }

    /**
     * Sets the gossip update interval.
     *
     * @param updateInterval the gossip update interval
     * @return the gossip member builder
     * @throws NullPointerException if the update interval is null
     */
    public Builder<K, V> withUpdateInterval(Duration updateInterval) {
      this.updateInterval = checkNotNull(updateInterval, "updateInterval cannot be null");
      return this;
    }

    /**
     * Sets whether to synchronously replicate updates.
     *
     * @param fastConvergence whether to synchronously replicate updates
     * @return the gossip member builder
     */
    public Builder<K, V> withFastConvergence(boolean fastConvergence) {
      this.fastConvergence = fastConvergence;
      return this;
    }

    /**
     * Sets whether to disable tombstones.
     *
     * @param tombstonesDisabled whether to disable tombstones
     * @return the gossip member builder
     */
    public Builder<K, V> withTombstonesDisabled(boolean tombstonesDisabled) {
      this.tombstonesDisabled = tombstonesDisabled;
      return this;
    }

    /**
     * Sets the tombstone purge interval.
     *
     * @param purgeInterval the tombstone purge interval
     * @return the gossip member builder
     * @throws NullPointerException if the purge interval is null
     */
    public Builder<K, V> withPurgeInterval(Duration purgeInterval) {
      this.purgeInterval = checkNotNull(purgeInterval, "purgeInterval cannot be null");
      return this;
    }
  }
}
