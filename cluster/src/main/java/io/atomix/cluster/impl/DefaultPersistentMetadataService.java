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
package io.atomix.cluster.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ClusterMetadataEvent;
import io.atomix.cluster.ClusterMetadataEventListener;
import io.atomix.cluster.ClusterMetadataService;
import io.atomix.cluster.ManagedPersistentMetadataService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.messaging.MessagingService;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.LogicalTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Default cluster metadata service.
 */
public class DefaultPersistentMetadataService
    extends AbstractListenerManager<ClusterMetadataEvent, ClusterMetadataEventListener>
    implements ManagedPersistentMetadataService {

  private static final String BOOTSTRAP_MESSAGE = "atomix-cluster-metadata-bootstrap";
  private static final String UPDATE_MESSAGE = "atomix-cluster-metadata-update";
  private static final String ADVERTISEMENT_MESSAGE = "atomix-cluster-metadata-advertisement";
  private static final int HEARTBEAT_INTERVAL = 1000;

  private static final Serializer SERIALIZER = Serializer.using(
      KryoNamespace.builder()
          .register(KryoNamespaces.BASIC)
          .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
          .register(ReplicatedMember.class)
          .register(MemberId.class)
          .register(Member.Type.class)
          .register(new AddressSerializer(), Address.class)
          .register(LogicalTimestamp.class)
          .register(MemberUpdate.class)
          .register(ClusterMetadataAdvertisement.class)
          .register(MemberDigest.class)
          .build("ClusterMetadataService"));

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final Map<MemberId, ReplicatedMember> nodes = Maps.newConcurrentMap();
  private final MessagingService messagingService;
  private final LogicalClock clock = new LogicalClock();
  private final AtomicBoolean started = new AtomicBoolean();

  private final ScheduledExecutorService messageScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-metadata-sender", log));
  private final ExecutorService messageExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-cluster-metadata-receiver", log));
  private ScheduledFuture<?> metadataFuture;

  public DefaultPersistentMetadataService(ClusterMetadata metadata, MessagingService messagingService) {
    metadata.members().forEach(node -> nodes.put(node.id(), new ReplicatedMember(
        node.id(),
        node.type(),
        node.address(),
        node.zone(),
        node.rack(),
        node.host(),
        node.metadata(),
        new LogicalTimestamp(0),
        false)));
    this.messagingService = messagingService;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ClusterMetadata getMetadata() {
    return new ClusterMetadata(ImmutableList.copyOf(nodes.values().stream()
        .filter(node -> !node.tombstone())
        .collect(Collectors.toList())));
  }

  @Override
  public void addMember(Member member) {
    if (member.type() == Member.Type.PERSISTENT) {
      ReplicatedMember replicatedNode = nodes.get(member.id());
      if (replicatedNode == null) {
        LogicalTimestamp timestamp = clock.increment();
        replicatedNode = new ReplicatedMember(
            member.id(),
            member.type(),
            member.address(),
            member.zone(),
            member.rack(),
            member.host(),
            member.metadata(),
            timestamp,
            false);
        nodes.put(replicatedNode.id(), replicatedNode);
        broadcastUpdate(new MemberUpdate(replicatedNode, timestamp));
        post(new ClusterMetadataEvent(ClusterMetadataEvent.Type.METADATA_CHANGED, getMetadata()));
      }
    }
  }

  @Override
  public void removeMember(Member member) {
    ReplicatedMember replicatedNode = nodes.get(member.id());
    if (replicatedNode != null) {
      LogicalTimestamp timestamp = clock.increment();
      replicatedNode = new ReplicatedMember(
          member.id(),
          member.type(),
          member.address(),
          member.zone(),
          member.rack(),
          member.host(),
          member.metadata(),
          timestamp,
          true);
      nodes.put(replicatedNode.id(), replicatedNode);
      broadcastUpdate(new MemberUpdate(replicatedNode, timestamp));
      post(new ClusterMetadataEvent(ClusterMetadataEvent.Type.METADATA_CHANGED, getMetadata()));
    }
  }

  /**
   * Bootstraps the cluster metadata.
   */
  private CompletableFuture<Void> bootstrap() {
    Set<Address> peers = nodes.values().stream()
        .map(Member::address)
        .filter(address -> !address.equals(messagingService.address()))
        .collect(Collectors.toSet());
    final int totalPeers = peers.size();
    if (totalPeers == 0) {
      return CompletableFuture.completedFuture(null);
    }

    AtomicBoolean successful = new AtomicBoolean();
    AtomicInteger totalCount = new AtomicInteger();
    AtomicReference<Throwable> lastError = new AtomicReference<>();

    // Iterate through all of the peers and send a bootstrap request. On the first peer that returns
    // a successful bootstrap response, complete the future. Otherwise, if no peers respond with any
    // successful bootstrap response, the future will be completed with the last exception.
    CompletableFuture<Void> future = new CompletableFuture<>();
    peers.forEach(peer -> {
      bootstrap(peer).whenComplete((result, error) -> {
        if (error == null) {
          if (successful.compareAndSet(false, true)) {
            future.complete(null);
          } else if (totalCount.incrementAndGet() == totalPeers) {
            Throwable e = lastError.get();
            if (e != null) {
              future.completeExceptionally(e);
            }
          }
        } else {
          if (!successful.get() && totalCount.incrementAndGet() == totalPeers) {
            future.completeExceptionally(error);
          } else {
            lastError.set(error);
          }
        }
      });
    });
    return future;
  }

  /**
   * Requests a bootstrap from the given address.
   */
  private CompletableFuture<Void> bootstrap(Address address) {
    return messagingService.sendAndReceive(address, BOOTSTRAP_MESSAGE, new byte[0])
        .thenAccept(response -> nodes.putAll(SERIALIZER.decode(response)));
  }

  /**
   * Handles a bootstrap request.
   */
  private byte[] handleBootstrap(Address address, byte[] payload) {
    return SERIALIZER.encode(nodes);
  }

  /**
   * Broadcasts the given update to all peers.
   */
  private void broadcastUpdate(MemberUpdate update) {
    nodes.values().stream()
        .map(Member::address)
        .filter(address -> !address.equals(messagingService.address()))
        .forEach(address -> sendUpdate(address, update));
  }

  /**
   * Sends the given update to the given node.
   */
  private void sendUpdate(Address address, MemberUpdate update) {
    messagingService.sendAsync(address, UPDATE_MESSAGE, SERIALIZER.encode(update));
  }

  /**
   * Handles an update from another node.
   */
  private void handleUpdate(Address address, byte[] payload) {
    MemberUpdate update = SERIALIZER.decode(payload);
    clock.incrementAndUpdate(update.timestamp());
    ReplicatedMember node = nodes.get(update.node().id());
    if (node == null || node.timestamp().isOlderThan(update.timestamp())) {
      nodes.put(update.node().id(), update.node());
      post(new ClusterMetadataEvent(ClusterMetadataEvent.Type.METADATA_CHANGED, getMetadata()));
    }
  }

  /**
   * Sends anti-entropy advertisements to a random node.
   */
  private void sendAdvertisement() {
    pickRandomPeer().ifPresent(this::sendAdvertisement);
  }

  /**
   * Sends an anti-entropy advertisement to the given node.
   */
  private void sendAdvertisement(Address address) {
    clock.increment();
    ClusterMetadataAdvertisement advertisement = new ClusterMetadataAdvertisement(
        Maps.newHashMap(Maps.transformValues(nodes, node -> new MemberDigest(node.timestamp(), node.tombstone()))));
    messagingService.sendAndReceive(address, ADVERTISEMENT_MESSAGE, SERIALIZER.encode(advertisement))
        .whenComplete((response, error) -> {
          if (error == null) {
            Set<MemberId> nodes = SERIALIZER.decode(response);
            for (MemberId memberId : nodes) {
              ReplicatedMember node = this.nodes.get(memberId);
              if (node != null) {
                sendUpdate(address, new MemberUpdate(node, clock.increment()));
              }
            }
          } else {
            log.warn("Anti-entropy advertisement to {} failed!", address);
          }
        });
  }

  /**
   * Selects a random peer to which to send an anti-entropy advertisement.
   */
  private Optional<Address> pickRandomPeer() {
    List<Address> nodes = this.nodes.values()
        .stream()
        .filter(replicatedNode -> !replicatedNode.tombstone() &&
            !replicatedNode.address().equals(messagingService.address()))
        .map(Member::address)
        .collect(Collectors.toList());
    Collections.shuffle(nodes);
    return nodes.stream().findFirst();
  }

  /**
   * Handles an anti-entropy advertisement.
   */
  private byte[] handleAdvertisement(Address address, byte[] payload) {
    LogicalTimestamp timestamp = clock.increment();
    ClusterMetadataAdvertisement advertisement = SERIALIZER.decode(payload);
    Set<MemberId> staleNodes = nodes.values().stream().map(node -> {
      MemberDigest digest = advertisement.digest(node.id());
      if (digest == null || node.isNewerThan(digest.timestamp())) {
        sendUpdate(address, new MemberUpdate(node, timestamp));
      } else if (digest.isNewerThan(node.timestamp())) {
        if (digest.tombstone()) {
          if (!node.tombstone()) {
            nodes.put(node.id(), new ReplicatedMember(
                node.id(),
                node.type(),
                node.address(),
                node.zone(),
                node.rack(),
                node.host(),
                node.metadata(),
                digest.timestamp(),
                true));
            post(new ClusterMetadataEvent(ClusterMetadataEvent.Type.METADATA_CHANGED, getMetadata()));
          }
        } else {
          return node.id();
        }
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toSet());
    return SERIALIZER.encode(Sets.newHashSet(Sets.union(Sets.difference(advertisement.digests(), nodes.keySet()), staleNodes)));
  }

  @Override
  public CompletableFuture<ClusterMetadataService> start() {
    if (started.compareAndSet(false, true)) {
      registerMessageHandlers();
      return bootstrap().exceptionally(e -> null).thenApply(result -> {
        metadataFuture = messageScheduler.scheduleWithFixedDelay(this::sendAdvertisement, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
        log.info("Started");
        return this;
      });
    }
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  /**
   * Registers cluster message handlers.
   */
  private void registerMessageHandlers() {
    messagingService.registerHandler(BOOTSTRAP_MESSAGE, this::handleBootstrap, messageExecutor);
    messagingService.registerHandler(UPDATE_MESSAGE, this::handleUpdate, messageExecutor);
    messagingService.registerHandler(ADVERTISEMENT_MESSAGE, this::handleAdvertisement, messageExecutor);
  }

  /**
   * Unregisters cluster message handlers.
   */
  private void unregisterMessageHandlers() {
    messagingService.unregisterHandler(BOOTSTRAP_MESSAGE);
    messagingService.unregisterHandler(UPDATE_MESSAGE);
    messagingService.unregisterHandler(ADVERTISEMENT_MESSAGE);
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (started.compareAndSet(true, false)) {
      messageScheduler.shutdownNow();
      messageExecutor.shutdownNow();
      metadataFuture.cancel(true);
      unregisterMessageHandlers();
    }
    log.info("Stopped");
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Address serializer.
   */
  static class AddressSerializer extends com.esotericsoftware.kryo.Serializer<Address> {
    @Override
    public void write(Kryo kryo, Output output, Address address) {
      output.writeString(address.address().getHostAddress());
      output.writeInt(address.port());
    }

    @Override
    public Address read(Kryo kryo, Input input, Class<Address> type) {
      String host = input.readString();
      int port = input.readInt();
      return Address.from(host, port);
    }
  }
}
