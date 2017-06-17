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
package io.atomix.protocols.gossip.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.cluster.NodeId;
import io.atomix.event.AbstractListenerManager;
import io.atomix.protocols.gossip.GossipEvent;
import io.atomix.protocols.gossip.GossipEventListener;
import io.atomix.protocols.gossip.GossipMember;
import io.atomix.protocols.gossip.protocol.GossipMessage;
import io.atomix.protocols.gossip.protocol.GossipProtocol;
import io.atomix.protocols.gossip.protocol.GossipUpdate;
import io.atomix.time.LogicalClock;
import io.atomix.time.LogicalTimestamp;
import io.atomix.time.Timestamp;
import io.atomix.time.Version;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
 * Default gossip member.
 */
public class DefaultGossipMember<K, V> extends AbstractListenerManager<GossipEvent<K, V>, GossipEventListener<K, V>> implements GossipMember<K, V> {
  private final GossipProtocol protocol;
  private final Supplier<Collection<NodeId>> peerProvider;
  private final Executor eventExecutor;
  private final boolean fastConvergence;
  private final boolean tombstonesDisabled;
  private final Duration purgeInterval;
  private final ScheduledFuture<?> updateFuture;
  private final ScheduledFuture<?> purgeFuture;
  private final Map<K, GossipEntry<K, V>> updates = Maps.newLinkedHashMap();
  private final LogicalClock logicalClock = new LogicalClock();
  private final Map<NodeId, LogicalTimestamp> peerUpdateTimes = new HashMap<>();

  public DefaultGossipMember(
      GossipProtocol protocol,
      Supplier<Collection<NodeId>> peerProvider,
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
    this.purgeInterval = checkNotNull(purgeInterval, "purgeInterval");
    protocol.listener().registerGossipListener(this::update);
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
    GossipEntry<K, V> update = new GossipEntry<>(
        event.subject(),
        event.value(),
        timestamp.asVersion(),
        timestamp,
        event.type() == GossipEvent.Type.DELETE);

    switch (event.type()) {
      // Always assume updates are the latest when performed on the local node.
      case UPDATE:
        updates.put(event.subject(), update);
        if (fastConvergence) {
          updatePeers();
        }
        break;
      case DELETE:
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
        break;
    }
    post(event);
  }

  /**
   * Handles a gossip message.
   */
  private synchronized void update(GossipMessage<K, V> message) {
    // Update the logical clock using the peer's logical time.
    Timestamp timestamp = logicalClock.incrementAndUpdate(message.timestamp());
    for (GossipUpdate<K, V> update : message.updates()) {
      GossipEntry<K, V> existingUpdate = updates.get(update.subject());

      // If no existing update is found, or if an update is overriding a tombstone, or if the update
      // version is greater than the existing entry version, perform the update.
      if (existingUpdate == null
          || (existingUpdate.tombstone && !update.isTombstone())
          || existingUpdate.version.isOlderThan(update.version())) {
        GossipEntry<K, V> entry = new GossipEntry<>(
            update.subject(),
            update.value(),
            update.version(),
            timestamp,
            update.isTombstone());

        // It's possible tombstones could just be disabled on this node.
        if (!tombstonesDisabled) {
          updates.put(update.subject(), entry);
        }

        // Post the event to listeners.
        post(new GossipEvent<>(
            entry.time,
            update.isTombstone() ? GossipEvent.Type.DELETE : GossipEvent.Type.UPDATE,
            update.subject(),
            update.value()));
      }
    }
  }

  /**
   * Sends a gossip message to a random peer.
   */
  private synchronized void gossip() {
    List<NodeId> nodes = Lists.newArrayList(peerProvider.get());
    if (!nodes.isEmpty()) {
      Collections.shuffle(nodes);
      NodeId node = nodes.get(0);
      updatePeer(node);
    }
  }

  /**
   * Updates all peers.
   */
  private void updatePeers() {
    for (NodeId peer : peerProvider.get()) {
      updatePeer(peer);
    }
  }

  /**
   * Updates the given peer.
   */
  private synchronized void updatePeer(NodeId peer) {
    // Increment the logical clock.
    LogicalTimestamp updateTime = logicalClock.increment();

    // Look up the last update time for the peer.
    LogicalTimestamp lastUpdate = peerUpdateTimes.computeIfAbsent(peer, n -> new LogicalTimestamp(0));

    // Filter updates based on the peer's last update time from this node.
    Collection<GossipUpdate<K, V>> filteredUpdates = updates.values().stream()
        .filter(update -> update.timestamp.isNewerThan(lastUpdate))
        .map(update -> new GossipUpdate<>(update.subject, update.value, update.version, update.tombstone))
        .collect(Collectors.toList());

    // Send the gossip message.
    protocol.dispatcher().gossip(peer, new GossipMessage<>(updateTime, filteredUpdates));

    // Set the peer's update time.
    peerUpdateTimes.put(peer, updateTime);
  }

  /**
   * Purges tombstones from updates.
   */
  private synchronized void purgeTombstones() {
    Iterator<Map.Entry<K, GossipEntry<K, V>>> iterator = updates.entrySet().iterator();
    while (iterator.hasNext()) {
      GossipEntry<K, V> update = iterator.next().getValue();
      if (update.tombstone && System.currentTimeMillis() - update.time > purgeInterval.toMillis()) {
        iterator.remove();
      }
    }
  }

  @Override
  public void close() {
    protocol.listener().unregisterGossipListener();
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
   * Internal gossip entry.
   */
  private static class GossipEntry<K, V> {
    private final K subject;
    private final V value;
    private final Version version;
    private final Timestamp timestamp;
    private final boolean tombstone;
    private final transient long time = System.currentTimeMillis();

    public GossipEntry(K subject, V value, Version version, Timestamp timestamp, boolean tombstone) {
      this.subject = subject;
      this.value = value;
      this.version = version;
      this.timestamp = timestamp;
      this.tombstone = tombstone;
    }
  }

  /**
   * Default gossip member builder.
   */
  public static class Builder<K, V> extends GossipMember.Builder<K, V> {
    @Override
    public GossipMember<K, V> build() {
      return new DefaultGossipMember<>(protocol, peerProvider, eventExecutor, communicationExecutor, updateInterval, fastConvergence, tombstonesDisabled, purgeInterval);
    }
  }
}
