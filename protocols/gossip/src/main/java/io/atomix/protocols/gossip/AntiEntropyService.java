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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.event.AbstractListenerManager;
import io.atomix.protocols.gossip.protocol.AntiEntropyAdvertisement;
import io.atomix.protocols.gossip.protocol.AntiEntropyProtocol;
import io.atomix.protocols.gossip.protocol.AntiEntropyResponse;
import io.atomix.protocols.gossip.protocol.GossipMessage;
import io.atomix.protocols.gossip.protocol.GossipUpdate;
import io.atomix.time.LogicalClock;
import io.atomix.utils.AbstractAccumulator;
import io.atomix.utils.Identifier;
import io.atomix.utils.SlidingWindowCounter;
import io.atomix.utils.concurrent.SingleThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Anti-entropy service.
 */
public class AntiEntropyService<K, V> extends AbstractListenerManager<GossipEvent<K, V>, GossipEventListener<K, V>> implements GossipService<K, V> {

  private static final int WINDOW_SIZE = 5;
  private static final int HIGH_LOAD_THRESHOLD = 2;
  private static final int LOAD_WINDOW = 2;

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final AntiEntropyProtocol<Identifier> protocol;
  private final Supplier<Collection<Identifier>> peerProvider;
  private final Executor eventExecutor;
  private final Executor communicationExecutor;

  private final boolean tombstonesDisabled;

  private final ScheduledFuture<?> updateFuture;
  private final ScheduledFuture<?> purgeFuture;

  private final Map<K, GossipUpdate<K, V>> updates = Maps.newLinkedHashMap();
  private final LogicalClock logicalClock = new LogicalClock();
  private final Map<Identifier, UpdateAccumulator> pendingUpdates = Maps.newConcurrentMap();
  private final Map<Identifier, Long> peerUpdateTimes = Maps.newConcurrentMap();

  private volatile boolean open = true;

  private final SlidingWindowCounter counter;

  public AntiEntropyService(
      AntiEntropyProtocol<Identifier> protocol,
      Supplier<Collection<Identifier>> peerProvider,
      Executor eventExecutor,
      ScheduledExecutorService communicationExecutor,
      Duration antiEntropyInterval,
      boolean tombstonesDisabled,
      Duration purgeInterval) {
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.peerProvider = checkNotNull(peerProvider, "peerProvider cannot be null");
    this.eventExecutor = checkNotNull(eventExecutor, "eventExecutor cannot be null");
    this.communicationExecutor = checkNotNull(communicationExecutor, "communicationExecutor cannot be null");
    this.counter = new SlidingWindowCounter(WINDOW_SIZE, new SingleThreadContext("anti-entropy-%d"));
    this.tombstonesDisabled = tombstonesDisabled;
    protocol.registerGossipListener(this::update);
    updateFuture = communicationExecutor.scheduleAtFixedRate(this::performAntiEntropy, 0, antiEntropyInterval.toMillis(), TimeUnit.MILLISECONDS);
    purgeFuture = !tombstonesDisabled ? communicationExecutor.scheduleAtFixedRate(this::purgeTombstones, 0, purgeInterval.toMillis(), TimeUnit.MILLISECONDS) : null;
  }

  @Override
  protected void post(GossipEvent<K, V> event) {
    eventExecutor.execute(() -> super.post(event));
  }

  @Override
  public void process(GossipEvent<K, V> event) {
    GossipUpdate<K, V> update = new GossipUpdate<>(
        event.subject(),
        event.value(),
        logicalClock.increment());

    if (update.isTombstone()) {
      // For deletes, simply remove the event if tombstones are disabled.
      // Otherwise, treat the tombstone as an update to ensure it's replicated.
      if (tombstonesDisabled) {
        updates.remove(update.subject());
      } else {
        updates.put(event.subject(), update);
        notifyPeers(update);
      }
    } else {
      updates.put(event.subject(), update);
      notifyPeers(update);
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
   * Returns the update accumulator for the given peer.
   *
   * @param peer the peer for which to return the accumulator
   * @return the update accumulator for the given peer
   */
  private UpdateAccumulator getAccumulator(Identifier peer) {
    return pendingUpdates.computeIfAbsent(peer, UpdateAccumulator::new);
  }

  /**
   * Returns a boolean indicating whether the service is under high load.
   *
   * @return indicates whether the service is under high load
   */
  private boolean underHighLoad() {
    return counter.get(LOAD_WINDOW) > HIGH_LOAD_THRESHOLD;
  }

  /**
   * Sends an anti-entropy advertisement to a random peer.
   */
  private void performAntiEntropy() {
    try {
      if (underHighLoad() || !open) {
        return;
      }
      pickRandomActivePeer().ifPresent(this::sendAdvertisementToPeer);
    } catch (Exception e) {
      // Catch all exceptions to avoid scheduled task being suppressed.
      log.error("Exception thrown while sending advertisement", e);
    }
  }

  /**
   * Picks a random active peer to which to send an anti-entropy advertisement.
   *
   * @return a random peer to which to send an anti-entropy advertisement
   */
  private Optional<Identifier> pickRandomActivePeer() {
    List<Identifier> peers = Lists.newArrayList(peerProvider.get());
    Collections.shuffle(peers);
    return peers.isEmpty() ? Optional.empty() : Optional.of(peers.get(0));
  }

  /**
   * Sends an anti-entropy advertisement to the given peer.
   *
   * @param peer the peer to which to send the anti-entropy advertisement
   */
  private void sendAdvertisementToPeer(Identifier peer) {
    long updateTime = System.currentTimeMillis();
    AntiEntropyAdvertisement<K> advertisement = new AntiEntropyAdvertisement<>(
        ImmutableMap.copyOf(Maps.transformValues(updates, GossipUpdate::digest)));
    protocol.advertise(peer, advertisement).whenComplete((response, error) -> {
      if (error != null) {
        log.debug("Failed to send anti-entropy advertisement to {}: {}", peer, error.getMessage());
      } else if (response.status() == AntiEntropyResponse.Status.PROCESSED) {
        if (!response.keys().isEmpty()) {
          UpdateAccumulator accumulator = getAccumulator(peer);
          for (K key : response.keys()) {
            GossipUpdate<K, V> update = updates.get(key);
            if (update != null) {
              accumulator.add(update);
            }
          }
        }
        peerUpdateTimes.put(peer, updateTime);
      }
    });
  }

  /**
   * Notifies peers of an update.
   *
   * @param event the event to send to peers
   */
  private void notifyPeers(GossipUpdate<K, V> event) {
    notifyPeers(event, peerProvider.get());
  }

  /**
   * Notifies peers of an update.
   *
   * @param event the event to send to the given peers
   * @param peers the peers to which to send the event
   */
  private void notifyPeers(GossipUpdate<K, V> event, Collection<Identifier> peers) {
    queueUpdate(event, peers);
  }

  /**
   * Queues an update to be sent to the given peers.
   *
   * @param event the event to send to the given peers
   * @param peers the peers to which to send the event
   */
  private void queueUpdate(GossipUpdate<K, V> event, Collection<Identifier> peers) {
    if (peers != null) {
      for (Identifier peer : peers) {
        getAccumulator(peer).add(event);
      }
    }
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
    open = false;
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

  private static final int DEFAULT_MAX_EVENTS = 1000;
  private static final int DEFAULT_MAX_IDLE_MS = 10;
  private static final int DEFAULT_MAX_BATCH_MS = 50;
  private static final Timer TIMER = new Timer("onos-ecm-sender-events");

  /**
   * Accumulator for dispatching updates to a peer.
   */
  private final class UpdateAccumulator extends AbstractAccumulator<GossipUpdate<K, V>> {
    private final Identifier peer;

    private UpdateAccumulator(Identifier peer) {
      super(TIMER, DEFAULT_MAX_EVENTS, DEFAULT_MAX_BATCH_MS, DEFAULT_MAX_IDLE_MS);
      this.peer = peer;
    }

    @Override
    public void processItems(List<GossipUpdate<K, V>> items) {
      Map<K, GossipUpdate<K, V>> map = Maps.newHashMap();
      items.forEach(item -> map.compute(item.subject(), (key, existing) ->
          item.timestamp().isNewerThan(existing.timestamp()) ? item : existing));
      communicationExecutor.execute(() -> {
        try {
          protocol.gossip(peer, new GossipMessage<>(logicalClock.increment(), map.values()));
        } catch (Exception e) {
          log.warn("Failed to send to {}", peer, e);
        }
      });
    }
  }

  /**
   * Gossip protocol builder.
   *
   * @param <K> the gossip subject type
   * @param <V> the gossip value type
   */
  public static class Builder<K, V> implements GossipService.Builder<K, V> {
    protected AntiEntropyProtocol protocol;
    protected Supplier<Collection<Identifier>> peerProvider;
    protected Executor eventExecutor = MoreExecutors.directExecutor();
    protected ScheduledExecutorService communicationExecutor;
    protected Duration antiEntropyInterval = Duration.ofSeconds(1);
    protected boolean tombstonesDisabled = false;
    protected Duration purgeInterval = Duration.ofMinutes(1);

    /**
     * Sets the anti-entropy protocol.
     *
     * @param protocol the anti-entropy protocol
     * @return the anti-entropy service builder
     * @throws NullPointerException if the protocol is null
     */
    public Builder<K, V> withProtocol(AntiEntropyProtocol protocol) {
      this.protocol = checkNotNull(protocol, "protocol");
      return this;
    }

    /**
     * Sets the gossip peer provider function.
     *
     * @param peerProvider the gossip peer provider
     * @return the anti-entropy service builder
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
     * @return the anti-entropy service builder
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
     * @return the anti-entropy service builder
     * @throws NullPointerException if the communication executor is null
     */
    public Builder<K, V> withCommunicationExecutor(ScheduledExecutorService executor) {
      this.communicationExecutor = checkNotNull(executor, "executor cannot be null");
      return this;
    }

    /**
     * Sets the anti-entropy interval.
     *
     * @param antiEntropyInterval the anti-entropy interval
     * @return the anti-entropy service builder
     * @throws NullPointerException if the anti-entropy interval is null
     */
    public Builder<K, V> withAntiEntropyInterval(Duration antiEntropyInterval) {
      this.antiEntropyInterval = checkNotNull(antiEntropyInterval, "antiEntropyInterval cannot be null");
      return this;
    }

    /**
     * Sets whether to disable tombstones.
     *
     * @param tombstonesDisabled whether to disable tombstones
     * @return the anti-entropy service builder
     */
    public Builder<K, V> withTombstonesDisabled(boolean tombstonesDisabled) {
      this.tombstonesDisabled = tombstonesDisabled;
      return this;
    }

    /**
     * Sets the tombstone purge interval.
     *
     * @param purgeInterval the tombstone purge interval
     * @return the anti-entropy service builder
     * @throws NullPointerException if the purge interval is null
     */
    public Builder<K, V> withPurgeInterval(Duration purgeInterval) {
      this.purgeInterval = checkNotNull(purgeInterval, "purgeInterval cannot be null");
      return this;
    }

    @Override
    public GossipService<K, V> build() {
      return new AntiEntropyService<>(protocol, peerProvider, eventExecutor, communicationExecutor, antiEntropyInterval, tombstonesDisabled, purgeInterval);
    }
  }
}
