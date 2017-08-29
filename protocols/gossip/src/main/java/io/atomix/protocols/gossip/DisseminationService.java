/*
 * Copyright 2017-present Open Networking Foundation
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.event.AbstractListenerManager;
import io.atomix.protocols.gossip.protocol.GossipMessage;
import io.atomix.protocols.gossip.protocol.GossipProtocol;
import io.atomix.protocols.gossip.protocol.GossipUpdate;
import io.atomix.time.LogicalClock;
import io.atomix.time.LogicalTimestamp;
import io.atomix.utils.Identifier;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Dissemination service.
 */
public class DisseminationService<K, V> extends AbstractListenerManager<GossipEvent<K, V>, GossipEventListener<K, V>> implements GossipService<K, V> {

  /**
   * Returns a new dissemination service builder.
   *
   * @param <K> the subject type
   * @param <V> the gossip value type
   * @return a new dissemination service builder
   */
  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  private final GossipProtocol protocol;
  private final Supplier<Collection<Identifier>> peerProvider;
  private final Executor eventExecutor;
  private final boolean fastConvergence;
  private final boolean tombstonesDisabled;
  private final ScheduledFuture<?> updateFuture;
  private final ScheduledFuture<?> purgeFuture;
  private final Map<K, GossipUpdate<K, V>> updates = Maps.newLinkedHashMap();
  private final LogicalClock logicalClock = new LogicalClock();
  private final Map<Identifier, Long> peerUpdateTimes = Maps.newConcurrentMap();
  private final Map<Identifier, LogicalTimestamp> peerTimestamps = Maps.newHashMap();

  public DisseminationService(
      GossipProtocol<?> protocol,
      Supplier<Collection<Identifier>> peerProvider,
      Executor eventExecutor,
      ScheduledExecutorService communicationExecutor,
      Duration updateInterval,
      boolean fastConvergence,
      boolean tombstonesDisabled,
      Duration purgeInterval) {
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.peerProvider = checkNotNull(peerProvider, "peerProvider cannot be null");
    this.eventExecutor = checkNotNull(eventExecutor, "eventExecutor cannot be null");
    this.fastConvergence = fastConvergence;
    this.tombstonesDisabled = tombstonesDisabled;
    protocol.registerGossipListener(this::update);
    updateFuture = communicationExecutor.scheduleAtFixedRate(this::gossip, 0, updateInterval.toMillis(), TimeUnit.MILLISECONDS);
    purgeFuture = !tombstonesDisabled ? communicationExecutor.scheduleAtFixedRate(this::purgeTombstones, 0, purgeInterval.toMillis(), TimeUnit.MILLISECONDS) : null;
  }

  @Override
  protected void post(GossipEvent<K, V> event) {
    eventExecutor.execute(() -> super.post(event));
  }

  @Override
  public void process(GossipEvent<K, V> event) {
    LogicalTimestamp timestamp = logicalClock.increment();
    GossipUpdate<K, V> update = new GossipUpdate<>(
        event.subject(),
        event.value(),
        timestamp.asVersion());

    if (event.value() != null) {
      updates.put(event.subject(), update);
      if (fastConvergence) {
        updatePeers();
      }
    } else {
      // For deletes, simply remove the event if tombstones are disabled.
      // Otherwise, treat the tombstone as an update to ensure it's replicated.
      if (tombstonesDisabled) {
        updates.remove(event.subject());
      } else {
        updates.put(event.subject(), update);
        if (fastConvergence) {
          updatePeers();
        }
      }
    }
    post(event);
  }

  /**
   * Handles a gossip message.
   */
  private synchronized void update(GossipMessage<K, V> message) {
    // Update the logical clock using the peer's logical time.
    logicalClock.update(message.timestamp());
    for (GossipUpdate<K, V> update : message.updates()) {
      GossipUpdate<K, V> existingUpdate = updates.get(update.subject());

      // If no existing update is found, or if an update is overriding a tombstone, or if the update
      // version is greater than the existing entry version, perform the update.
      if (existingUpdate == null
          || (existingUpdate.isTombstone() && !update.isTombstone())
          || existingUpdate.timestamp().isOlderThan(update.timestamp())) {
        // It's possible tombstones could just be disabled on this node.
        if (!tombstonesDisabled) {
          updates.put(update.subject(), update);
        }

        // Post the event to listeners.
        post(new GossipEvent<>(
            update.creationTime(),
            update.subject(),
            update.value()));
      }
    }
  }

  /**
   * Sends a gossip message to a random peer.
   */
  private synchronized void gossip() {
    List<Identifier> peers = Lists.newArrayList(peerProvider.get());
    if (!peers.isEmpty()) {
      Collections.shuffle(peers);
      Identifier peer = peers.get(0);
      updatePeer(peer);
    }
  }

  /**
   * Updates all peers.
   */
  private void updatePeers() {
    for (Identifier peer : peerProvider.get()) {
      updatePeer(peer);
    }
  }

  /**
   * Updates the given peer.
   */
  private synchronized void updatePeer(Identifier peer) {
    // Increment the logical clock.
    LogicalTimestamp updateTimestamp = logicalClock.increment();

    // Store the update time.
    long updateTime = System.currentTimeMillis();

    // Look up the last update time for the peer.
    LogicalTimestamp lastUpdate = peerTimestamps.computeIfAbsent(peer, n -> new LogicalTimestamp(0));

    // Filter updates based on the peer's last update time from this node.
    Collection<GossipUpdate<K, V>> filteredUpdates = updates.values().stream()
        .filter(update -> update.timestamp().isNewerThan(lastUpdate))
        .collect(Collectors.toList());

    // Send the gossip message.
    protocol.gossip(peer, new GossipMessage<>(updateTimestamp, filteredUpdates));

    // Set the peer's update time.
    peerTimestamps.put(peer, updateTimestamp);
    peerUpdateTimes.put(peer, updateTime);
  }

  /**
   * Purges tombstones from updates.
   */
  private synchronized void purgeTombstones() {
    long minTombstoneTime = peerProvider.get().stream()
        .map(peer -> peerUpdateTimes.getOrDefault(peer, 0L))
        .reduce(Math::min)
        .orElse(0L);
    Iterator<Map.Entry<K, GossipUpdate<K, V>>> iterator = updates.entrySet().iterator();
    while (iterator.hasNext()) {
      GossipUpdate<K, V> update = iterator.next().getValue();
      if (update.isTombstone() && update.creationTime() < minTombstoneTime) {
        iterator.remove();
      }
    }
  }

  @Override
  public void close() {
    protocol.unregisterGossipListener();
    updateFuture.cancel(false);
    if (purgeFuture != null) {
      purgeFuture.cancel(false);
    }
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("protocol", protocol)
        .toString();
  }

  /**
   * Dissemination protocol builder.
   *
   * @param <K> the gossip subject type
   * @param <V> the gossip value type
   */
  public static class Builder<K, V> implements GossipService.Builder<K, V> {
    protected GossipProtocol protocol;
    protected Supplier<Collection<Identifier>> peerProvider;
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
     * @return the dissemination service builder
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
     * @return the dissemination service builder
     * @throws NullPointerException if the peer provider is null
     */
    public Builder<K, V> withPeerProvider(Supplier<Collection<Identifier>> peerProvider) {
      this.peerProvider = checkNotNull(peerProvider, "peerProvider cannot be null");
      return this;
    }

    /**
     * Sets the gossip event executor.
     *
     * @param executor the gossip event executor
     * @return the dissemination service builder
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
     * @return the dissemination service builder
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
     * @return the dissemination service builder
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
     * @return the dissemination service builder
     */
    public Builder<K, V> withFastConvergence(boolean fastConvergence) {
      this.fastConvergence = fastConvergence;
      return this;
    }

    /**
     * Sets whether to disable tombstones.
     *
     * @param tombstonesDisabled whether to disable tombstones
     * @return the dissemination service builder
     */
    public Builder<K, V> withTombstonesDisabled(boolean tombstonesDisabled) {
      this.tombstonesDisabled = tombstonesDisabled;
      return this;
    }

    /**
     * Sets the tombstone purge interval.
     *
     * @param purgeInterval the tombstone purge interval
     * @return the dissemination service builder
     * @throws NullPointerException if the purge interval is null
     */
    public Builder<K, V> withPurgeInterval(Duration purgeInterval) {
      this.purgeInterval = checkNotNull(purgeInterval, "purgeInterval cannot be null");
      return this;
    }

    @Override
    public GossipService<K, V> build() {
      return new DisseminationService<>(protocol, peerProvider, eventExecutor, communicationExecutor, updateInterval, fastConvergence, tombstonesDisabled, purgeInterval);
    }
  }
}
