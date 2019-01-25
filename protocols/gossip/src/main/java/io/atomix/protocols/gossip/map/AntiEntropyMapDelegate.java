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
package io.atomix.protocols.gossip.map;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.map.MapDelegate;
import io.atomix.primitive.protocol.map.MapDelegateEvent;
import io.atomix.primitive.protocol.map.MapDelegateEventListener;
import io.atomix.protocols.gossip.AntiEntropyProtocolConfig;
import io.atomix.protocols.gossip.PeerSelector;
import io.atomix.protocols.gossip.TimestampProvider;
import io.atomix.utils.concurrent.AbstractAccumulator;
import io.atomix.utils.misc.SlidingWindowCounter;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.Timestamp;
import io.atomix.utils.time.WallClockTimestamp;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.atomix.primitive.protocol.map.MapDelegateEvent.Type.INSERT;
import static io.atomix.primitive.protocol.map.MapDelegateEvent.Type.REMOVE;
import static io.atomix.primitive.protocol.map.MapDelegateEvent.Type.UPDATE;
import static io.atomix.utils.concurrent.Threads.namedThreads;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class AntiEntropyMapDelegate<K, V> implements MapDelegate<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AntiEntropyMapDelegate.class);
  private static final String ERROR_DESTROYED = " map is already destroyed";
  private static final String ERROR_NULL_KEY = "Key cannot be null";
  private static final String ERROR_NULL_VALUE = "Null values are not allowed";
  private static final int WINDOW_SIZE = 5;
  private static final int HIGH_LOAD_THRESHOLD = 2;
  private static final int LOAD_WINDOW = 2;

  private final Map<String, MapValue> items;
  private final ClusterCommunicationService clusterCommunicator;
  private final ClusterMembershipService membershipService;
  private final Serializer entrySerializer;
  private final Serializer serializer;
  private final TimestampProvider<Map.Entry<K, V>> timestampProvider;
  private final String bootstrapMessageSubject;
  private final String initializeMessageSubject;
  private final String updateMessageSubject;
  private final String antiEntropyAdvertisementSubject;
  private final String updateRequestSubject;
  private final Set<MapDelegateEventListener<K, V>> listeners = Sets.newCopyOnWriteArraySet();
  private final ExecutorService executor;
  private final ScheduledExecutorService backgroundExecutor;
  private final PeerSelector<Map.Entry<K, V>> peerUpdateFunction;
  private final ExecutorService communicationExecutor;
  private final Map<MemberId, EventAccumulator> senderPending;
  private final Map<MemberId, Long> antiEntropyTimes = Maps.newConcurrentMap();
  private final String mapName;
  private final String destroyedMessage;
  private final long initialDelaySec = 5;
  private final boolean tombstonesDisabled;
  private final Supplier<List<MemberId>> peersSupplier;
  private final Supplier<List<MemberId>> bootstrapPeersSupplier;
  private final MemberId localMemberId;
  private long previousTombstonePurgeTime;
  private volatile boolean closed = false;
  private SlidingWindowCounter counter = new SlidingWindowCounter(WINDOW_SIZE);

  public AntiEntropyMapDelegate(String name, Serializer entrySerializer, AntiEntropyProtocolConfig config, PrimitiveManagementService managementService) {
    this.localMemberId = managementService.getMembershipService().getLocalMember().id();
    this.mapName = name;
    this.entrySerializer = entrySerializer;
    this.serializer = Serializer.using(Namespace.builder()
        .nextId(Namespaces.BEGIN_USER_CUSTOM_ID + 100)
        .register(Namespaces.BASIC)
        .register(LogicalTimestamp.class)
        .register(WallClockTimestamp.class)
        .register(AntiEntropyAdvertisement.class)
        .register(AntiEntropyResponse.class)
        .register(UpdateEntry.class)
        .register(MapValue.class)
        .register(MapValue.Digest.class)
        .register(UpdateRequest.class)
        .register(MemberId.class)
        .build(name + "-anti-entropy-map"));
    this.items = Maps.newConcurrentMap();
    senderPending = Maps.newConcurrentMap();
    destroyedMessage = mapName + ERROR_DESTROYED;

    this.clusterCommunicator = managementService.getCommunicationService();
    this.membershipService = managementService.getMembershipService();

    this.timestampProvider = config.getTimestampProvider();

    List<MemberId> peers = config.getPeers() != null ? config.getPeers().stream().map(MemberId::from).collect(Collectors.toList()) : null;
    this.peersSupplier = () -> peers != null ? peers
        : managementService.getMembershipService()
            .getMembers()
            .stream()
            .map(Member::id)
            .sorted()
            .collect(Collectors.toList());
    this.bootstrapPeersSupplier = peersSupplier;

    PeerSelector<Map.Entry<K, V>> peerSelector = config.getPeerSelector();
    this.peerUpdateFunction = (entry, m) -> {
      Collection<MemberId> selected = peerSelector.select(entry, m);
      return peersSupplier.get()
          .stream()
          .filter(selected::contains)
          .collect(Collectors.toList());
    };

    this.executor = newFixedThreadPool(8, namedThreads("atomix-anti-entropy-map-" + mapName + "-fg-%d", LOGGER));
    this.communicationExecutor = newFixedThreadPool(8, namedThreads("atomix-anti-entropy-map-" + mapName + "-publish-%d", LOGGER));
    this.backgroundExecutor = newSingleThreadScheduledExecutor(namedThreads("atomix-anti-entropy-map-" + mapName + "-bg-%d", LOGGER));

    // start anti-entropy thread
    this.backgroundExecutor.scheduleAtFixedRate(
        this::sendAdvertisement,
        initialDelaySec,
        config.getAntiEntropyInterval().toMillis(),
        TimeUnit.MILLISECONDS);

    bootstrapMessageSubject = "atomix-gossip-map-" + mapName + "-bootstrap";
    clusterCommunicator.subscribe(
        bootstrapMessageSubject,
        serializer::decode,
        (Function<MemberId, CompletableFuture<Void>>) this::handleBootstrap,
        serializer::encode
    );

    initializeMessageSubject = "atomix-gossip-map-" + mapName + "-initialize";
    clusterCommunicator.subscribe(
        initializeMessageSubject,
        serializer::decode,
        (Function<Collection<UpdateEntry>, Void>) u -> {
          processUpdates(u);
          return null;
        },
        serializer::encode,
        this.executor
    );

    updateMessageSubject = "atomix-gossip-map-" + mapName + "-update";
    clusterCommunicator.subscribe(
        updateMessageSubject,
        serializer::decode,
        this::processUpdates,
        this.executor
    );

    antiEntropyAdvertisementSubject = "atomix-gossip-map-" + mapName + "-anti-entropy";
    clusterCommunicator.subscribe(
        antiEntropyAdvertisementSubject,
        serializer::decode,
        this::handleAntiEntropyAdvertisement,
        serializer::encode,
        this.backgroundExecutor
    );

    updateRequestSubject = "atomix-gossip-map-" + mapName + "-update-request";
    clusterCommunicator.subscribe(
        updateRequestSubject,
        serializer::decode,
        this::handleUpdateRequests,
        this.backgroundExecutor
    );

    if (!config.isTombstonesDisabled()) {
      previousTombstonePurgeTime = 0;
      this.backgroundExecutor.scheduleWithFixedDelay(
          this::purgeTombstones,
          initialDelaySec,
          config.getAntiEntropyInterval().toMillis(),
          TimeUnit.MILLISECONDS
      );
    }

    this.tombstonesDisabled = config.isTombstonesDisabled();

    // Initiate first round of Gossip
    this.bootstrap();
  }

  private String encodeKey(Object key) {
    return BaseEncoding.base16().encode(entrySerializer.encode(key));
  }

  private byte[] encodeValue(Object value) {
    return value != null ? entrySerializer.encode(value) : null;
  }

  private K decodeKey(String key) {
    return entrySerializer.decode(BaseEncoding.base16().decode(key));
  }

  private V decodeValue(byte[] value) {
    return value != null ? entrySerializer.decode(value) : null;
  }

  @Override
  public int size() {
    checkState(!closed, destroyedMessage);
    // TODO: Maintain a separate counter for tracking live elements in map.
    return Maps.filterValues(items, MapValue::isAlive).size();
  }

  @Override
  public boolean isEmpty() {
    checkState(!closed, destroyedMessage);
    return size() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    checkState(!closed, destroyedMessage);
    checkNotNull(key, ERROR_NULL_KEY);
    return get(key) != null;
  }

  @Override
  public boolean containsValue(Object value) {
    checkState(!closed, destroyedMessage);
    checkNotNull(value, ERROR_NULL_VALUE);
    return items.values()
        .stream()
        .filter(MapValue::isAlive)
        .anyMatch(v -> Arrays.equals(encodeValue(value), v.get()));
  }

  @Override
  public V get(Object key) {
    checkState(!closed, destroyedMessage);
    checkNotNull(key, ERROR_NULL_KEY);

    MapValue value = items.get(encodeKey(key));
    return (value == null || value.isTombstone()) ? null : value.get(this::decodeValue);
  }

  @Override
  public V put(K key, V value) {
    checkState(!closed, destroyedMessage);
    checkNotNull(key, ERROR_NULL_KEY);
    checkNotNull(value, ERROR_NULL_VALUE);

    String encodedKey = encodeKey(key);
    byte[] encodedValue = encodeValue(value);

    MapValue newValue = new MapValue(encodedValue, timestampProvider.get(Maps.immutableEntry(key, value)));

    counter.incrementCount();
    AtomicReference<byte[]> oldValue = new AtomicReference<>();
    AtomicBoolean updated = new AtomicBoolean(false);
    items.compute(encodedKey, (k, existing) -> {
      if (existing == null || newValue.isNewerThan(existing)) {
        updated.set(true);
        oldValue.set(existing != null ? existing.get() : null);
        return newValue;
      }
      return existing;
    });

    if (updated.get()) {
      notifyPeers(new UpdateEntry(encodedKey, newValue), peerUpdateFunction.select(Maps.immutableEntry(key, value), membershipService));
      if (oldValue.get() == null) {
        notifyListeners(new MapDelegateEvent<>(INSERT, key, value));
      } else {
        notifyListeners(new MapDelegateEvent<>(UPDATE, key, value));
      }
      return decodeValue(oldValue.get());
    }
    return value;
  }

  @Override
  public V remove(Object key) {
    checkState(!closed, destroyedMessage);
    checkNotNull(key, ERROR_NULL_KEY);
    return removeAndNotify((K) key, null);
  }

  @Override
  public boolean remove(Object key, Object value) {
    checkState(!closed, destroyedMessage);
    checkNotNull(key, ERROR_NULL_KEY);
    checkNotNull(value, ERROR_NULL_VALUE);
    return removeAndNotify((K) key, (V) value) != null;
  }

  private V removeAndNotify(K key, V value) {
    String encodedKey = encodeKey(key);
    byte[] encodedValue = encodeValue(value);
    Timestamp timestamp = timestampProvider.get(Maps.immutableEntry(key, value));
    Optional<MapValue> tombstone = tombstonesDisabled || timestamp == null
        ? Optional.empty() : Optional.of(MapValue.tombstone(timestamp));
    MapValue previousValue = removeInternal(encodedKey, Optional.ofNullable(encodedValue), tombstone);
    V decodedPreviousValue = null;
    if (previousValue != null) {
      decodedPreviousValue = previousValue.get(this::decodeValue);
      notifyPeers(new UpdateEntry(encodedKey, tombstone.orElse(null)),
          peerUpdateFunction.select(Maps.immutableEntry(key, decodedPreviousValue), membershipService));
      if (previousValue.isAlive()) {
        notifyListeners(new MapDelegateEvent<>(REMOVE, key, decodedPreviousValue));
      }
    }
    return decodedPreviousValue;
  }

  private MapValue removeInternal(String key, Optional<byte[]> value, Optional<MapValue> tombstone) {
    checkState(!closed, destroyedMessage);
    checkNotNull(key, ERROR_NULL_KEY);
    checkNotNull(value, ERROR_NULL_VALUE);
    tombstone.ifPresent(v -> checkState(v.isTombstone()));

    counter.incrementCount();
    AtomicBoolean updated = new AtomicBoolean(false);
    AtomicReference<MapValue> previousValue = new AtomicReference<>();
    items.compute(key, (k, existing) -> {
      boolean valueMatches = true;
      if (value.isPresent() && existing != null && existing.isAlive()) {
        valueMatches = Arrays.equals(value.get(), existing.get());
      }
      if (existing == null) {
        LOGGER.trace("ECMap Remove: Existing value for key {} is already null", k);
      }
      if (valueMatches) {
        if (existing == null) {
          updated.set(tombstone.isPresent());
        } else {
          updated.set(!tombstone.isPresent() || tombstone.get().isNewerThan(existing));
        }
      }
      if (updated.get()) {
        previousValue.set(existing);
        return tombstone.orElse(null);
      } else {
        return existing;
      }
    });
    return previousValue.get();
  }

  @Override
  public V compute(K key, BiFunction<? super K, ? super V, ? extends V> recomputeFunction) {
    checkState(!closed, destroyedMessage);
    checkNotNull(key, ERROR_NULL_KEY);
    checkNotNull(recomputeFunction, "Recompute function cannot be null");

    String encodedKey = encodeKey(key);
    AtomicReference<MapDelegateEvent.Type> update = new AtomicReference<>();
    AtomicReference<MapValue> previousValue = new AtomicReference<>();
    MapValue computedValue = items.compute(encodedKey, (k, mv) -> {
      previousValue.set(mv);
      V newRawValue = recomputeFunction.apply(key, mv == null ? null : mv.get(this::decodeValue));
      byte[] newEncodedValue = encodeValue(newRawValue);
      if (mv != null && Arrays.equals(newEncodedValue, mv.get())) {
        // value was not updated
        return mv;
      }
      MapValue newValue = new MapValue(newEncodedValue, timestampProvider.get(Maps.immutableEntry(key, newRawValue)));
      if (mv == null) {
        update.set(INSERT);
        return newValue;
      } else if (newValue.isNewerThan(mv)) {
        update.set(UPDATE);
        return newValue;
      } else {
        return mv;
      }
    });
    if (update.get() != null) {
      notifyPeers(new UpdateEntry(encodedKey, computedValue), peerUpdateFunction.select(Maps.immutableEntry(key, computedValue.get(this::decodeValue)), membershipService));
      MapDelegateEvent.Type updateType = computedValue.isTombstone() ? REMOVE : update.get();
      V value = computedValue.isTombstone()
          ? previousValue.get() == null ? null : previousValue.get().get(this::decodeValue)
          : computedValue.get(this::decodeValue);
      if (value != null) {
        notifyListeners(new MapDelegateEvent<>(updateType, key, value));
      }
      return value;
    }
    return computedValue.get(this::decodeValue);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    checkState(!closed, destroyedMessage);
    m.forEach(this::put);
  }

  @Override
  public void clear() {
    checkState(!closed, destroyedMessage);
    Maps.filterValues(items, MapValue::isAlive)
        .forEach((k, v) -> remove(decodeKey(k)));
  }

  @Override
  public Set<K> keySet() {
    checkState(!closed, destroyedMessage);
    return Maps.filterValues(items, MapValue::isAlive).keySet()
        .stream()
        .map(this::decodeKey)
        .collect(Collectors.toSet());
  }

  @Override
  public Collection<V> values() {
    checkState(!closed, destroyedMessage);
    return Collections2.transform(Maps.filterValues(items, MapValue::isAlive).values(), value -> value.get(this::decodeValue));
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    checkState(!closed, destroyedMessage);
    return Maps.filterValues(items, MapValue::isAlive)
        .entrySet()
        .stream()
        .map(e -> Pair.of(decodeKey(e.getKey()), decodeValue(e.getValue().get())))
        .collect(Collectors.toSet());
  }

  @Override
  public void addListener(MapDelegateEventListener<K, V> listener) {
    checkState(!closed, destroyedMessage);

    listeners.add(checkNotNull(listener));
    items.forEach((k, v) -> {
      if (v.isAlive()) {
        listener.event(new MapDelegateEvent<>(INSERT, decodeKey(k), v.get(this::decodeValue)));
      }
    });
  }

  @Override
  public void removeListener(MapDelegateEventListener<K, V> listener) {
    checkState(!closed, destroyedMessage);

    listeners.remove(checkNotNull(listener));
  }

  @Override
  public void close() {
    closed = true;

    executor.shutdown();
    backgroundExecutor.shutdown();
    communicationExecutor.shutdown();

    listeners.clear();

    clusterCommunicator.unsubscribe(bootstrapMessageSubject);
    clusterCommunicator.unsubscribe(initializeMessageSubject);
    clusterCommunicator.unsubscribe(updateMessageSubject);
    clusterCommunicator.unsubscribe(updateRequestSubject);
    clusterCommunicator.unsubscribe(antiEntropyAdvertisementSubject);
  }

  private void notifyListeners(MapDelegateEvent<K, V> event) {
    listeners.forEach(listener -> listener.event(event));
  }

  private void notifyPeers(UpdateEntry event, Collection<MemberId> peers) {
    queueUpdate(event, peers);
  }

  private void queueUpdate(UpdateEntry event, Collection<MemberId> peers) {
    if (peers == null) {
      // we have no friends :(
      return;
    }
    peers.forEach(node ->
        senderPending.computeIfAbsent(node, unusedKey -> new EventAccumulator(node)).add(event)
    );
  }

  private boolean underHighLoad() {
    return counter.get(LOAD_WINDOW) > HIGH_LOAD_THRESHOLD;
  }

  private void sendAdvertisement() {
    try {
      if (underHighLoad() || closed) {
        return;
      }
      pickRandomActivePeer().ifPresent(this::sendAdvertisementToPeer);
    } catch (Exception e) {
      // Catch all exceptions to avoid scheduled task being suppressed.
      LOGGER.error("Exception thrown while sending advertisement", e);
    }
  }

  private Optional<MemberId> pickRandomActivePeer() {
    List<MemberId> activePeers = peersSupplier.get();
    Collections.shuffle(activePeers);
    return activePeers.stream().findFirst();
  }

  private void sendAdvertisementToPeer(MemberId peer) {
    long adCreationTime = System.currentTimeMillis();
    AntiEntropyAdvertisement ad = createAdvertisement();
    clusterCommunicator.send(
        antiEntropyAdvertisementSubject,
        ad,
        serializer::encode,
        serializer::decode,
        peer)
        .whenComplete((result, error) -> {
          if (error != null) {
            LOGGER.debug("Failed to send anti-entropy advertisement to {}: {}",
                peer, error.getMessage());
          } else if (result == AntiEntropyResponse.PROCESSED) {
            antiEntropyTimes.put(peer, adCreationTime);
          }
        });
  }

  private void sendUpdateRequestToPeer(MemberId peer, Set<String> keys) {
    UpdateRequest<String> request = new UpdateRequest<>(localMemberId, keys);
    clusterCommunicator.unicast(
        updateRequestSubject,
        request,
        serializer::encode,
        peer)
        .whenComplete((result, error) -> {
          if (error != null) {
            LOGGER.debug("Failed to send update request to {}: {}",
                peer, error.getMessage());
          }
        });
  }

  private AntiEntropyAdvertisement createAdvertisement() {
    return new AntiEntropyAdvertisement(localMemberId,
        ImmutableMap.copyOf(Maps.transformValues(items, MapValue::digest)));
  }

  private AntiEntropyResponse handleAntiEntropyAdvertisement(AntiEntropyAdvertisement ad) {
    if (closed || underHighLoad()) {
      return AntiEntropyResponse.IGNORED;
    }
    try {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Received anti-entropy advertisement from {} for {} with {} entries in it",
            ad.sender(), mapName, ad.digest().size());
      }
      antiEntropyCheckLocalItems(ad).forEach(this::notifyListeners);
    } catch (Exception e) {
      LOGGER.warn("Error handling anti-entropy advertisement", e);
      return AntiEntropyResponse.FAILED;
    }
    return AntiEntropyResponse.PROCESSED;
  }

  /**
   * Processes anti-entropy ad from peer by taking following actions: 1. If peer has an old entry, updates peer. 2. If
   * peer indicates an entry is removed and has a more recent timestamp than the local entry, update local state.
   */
  private List<MapDelegateEvent<K, V>> antiEntropyCheckLocalItems(AntiEntropyAdvertisement ad) {
    final List<MapDelegateEvent<K, V>> externalEvents = Lists.newLinkedList();
    final MemberId sender = ad.sender();
    final List<MemberId> peers = ImmutableList.of(sender);
    Set<String> staleOrMissing = new HashSet<>();
    Set<String> locallyUnknown = new HashSet<>(ad.digest().keySet());

    items.forEach((key, localValue) -> {
      locallyUnknown.remove(key);
      MapValue.Digest remoteValueDigest = ad.digest().get(key);
      if (remoteValueDigest == null || localValue.isNewerThan(remoteValueDigest.timestamp())) {
        // local value is more recent, push to sender
        queueUpdate(new UpdateEntry(key, localValue), peers);
      } else if (remoteValueDigest.isNewerThan(localValue.digest()) && remoteValueDigest.isTombstone()) {
        // remote value is more recent and a tombstone: update local value
        MapValue tombstone = MapValue.tombstone(remoteValueDigest.timestamp());
        MapValue previousValue = removeInternal(key,
            Optional.empty(),
            Optional.of(tombstone));
        if (previousValue != null && previousValue.isAlive()) {
          externalEvents.add(new MapDelegateEvent<>(REMOVE, decodeKey(key), previousValue.get(this::decodeValue)));
        }
      } else if (remoteValueDigest.isNewerThan(localValue.digest())) {
        // Not a tombstone and remote is newer
        staleOrMissing.add(key);
      }
    });
    // Keys missing in local map
    staleOrMissing.addAll(locallyUnknown);
    // Request updates that we missed out on
    sendUpdateRequestToPeer(sender, staleOrMissing);
    return externalEvents;
  }

  private void handleUpdateRequests(UpdateRequest<String> request) {
    final Set<String> keys = request.keys();
    final MemberId sender = request.sender();
    final List<MemberId> peers = ImmutableList.of(sender);
    keys.forEach(key ->
        queueUpdate(new UpdateEntry(key, items.get(key)), peers)
    );
  }

  private void purgeTombstones() {
    /*
     * In order to mitigate the resource exhaustion that can ensue due to an ever-growing set
     * of tombstones we employ the following heuristic to purge old tombstones periodically.
     * First, we keep track of the time (local system time) when we were able to have a successful
     * AE exchange with each peer. The smallest (or oldest) such time across *all* peers is regarded
     * as the time before which all tombstones are considered safe to purge.
     */
    long currentSafeTombstonePurgeTime = peersSupplier.get()
        .stream()
        .map(id -> antiEntropyTimes.getOrDefault(id, 0L))
        .reduce(Math::min)
        .orElse(0L);
    if (currentSafeTombstonePurgeTime == previousTombstonePurgeTime) {
      return;
    }
    List<Map.Entry<String, MapValue>> tombStonesToDelete = items.entrySet()
        .stream()
        .filter(e -> e.getValue().isTombstone())
        .filter(e -> e.getValue().creationTime() <= currentSafeTombstonePurgeTime)
        .collect(Collectors.toList());
    previousTombstonePurgeTime = currentSafeTombstonePurgeTime;
    tombStonesToDelete.forEach(entry -> items.remove(entry.getKey(), entry.getValue()));
  }

  private void processUpdates(Collection<UpdateEntry> updates) {
    if (closed) {
      return;
    }
    updates.forEach(update -> {
      final String key = update.key();
      final MapValue value = update.value() == null ? null : update.value().copy();
      if (value == null || value.isTombstone()) {
        MapValue previousValue = removeInternal(key, Optional.empty(), Optional.ofNullable(value));
        if (previousValue != null && previousValue.isAlive()) {
          notifyListeners(new MapDelegateEvent<>(REMOVE, decodeKey(key), previousValue.get(this::decodeValue)));
        }
      } else {
        counter.incrementCount();
        AtomicReference<byte[]> oldValue = new AtomicReference<>();
        AtomicBoolean updated = new AtomicBoolean(false);
        items.compute(key, (k, existing) -> {
          if (existing == null || value.isNewerThan(existing)) {
            updated.set(true);
            oldValue.set(existing != null ? existing.get() : null);
            return value;
          }
          return existing;
        });

        if (updated.get()) {
          if (oldValue.get() == null) {
            notifyListeners(new MapDelegateEvent<>(INSERT, decodeKey(key), decodeValue(value.get())));
          } else {
            notifyListeners(new MapDelegateEvent<>(UPDATE, decodeKey(key), decodeValue(value.get())));
          }
        }
      }
    });
  }

  /**
   * Bootstraps the map to attempt to get in sync with existing instances of the same map on other nodes in the cluster.
   * This is necessary to ensure that a read immediately after the map is created doesn't return a null value.
   */
  private void bootstrap() {
    List<MemberId> activePeers = bootstrapPeersSupplier.get();
    if (activePeers.isEmpty()) {
      return;
    }
    try {
      requestBootstrapFromPeers(activePeers)
          .get(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      LOGGER.debug("Failed to bootstrap ec map {}: {}", mapName, ExceptionUtils.getStackTrace(e));
    }
  }

  /**
   * Requests all updates from each peer in the provided list of peers.
   * <p>
   * The returned future will be completed once at least one peer bootstraps this map or bootstrap requests to all peers
   * fail.
   *
   * @param peers the list of peers from which to request updates
   * @return a future to be completed once updates have been received from at least one peer
   */
  private CompletableFuture<Void> requestBootstrapFromPeers(List<MemberId> peers) {
    if (peers.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    final int totalPeers = peers.size();
    AtomicBoolean successful = new AtomicBoolean();
    AtomicInteger totalCount = new AtomicInteger();
    AtomicReference<Throwable> lastError = new AtomicReference<>();

    // Iterate through all of the peers and send a bootstrap request. On the first peer that returns
    // a successful bootstrap response, complete the future. Otherwise, if no peers respond with any
    // successful bootstrap response, the future will be completed with the last exception.
    for (MemberId peer : peers) {
      requestBootstrapFromPeer(peer).whenComplete((result, error) -> {
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
    }
    return future;
  }

  /**
   * Requests a bootstrap from the given peer.
   *
   * @param peer the peer from which to request updates
   * @return a future to be completed once the peer has sent bootstrap updates
   */
  private CompletableFuture<Void> requestBootstrapFromPeer(MemberId peer) {
    LOGGER.trace("Sending bootstrap request to {} for {}", peer, mapName);
    return clusterCommunicator.<MemberId, Void>send(
        bootstrapMessageSubject,
        localMemberId,
        serializer::encode,
        serializer::decode,
        peer)
        .whenComplete((updates, error) -> {
          if (error != null) {
            LOGGER.debug("Bootstrap request to {} failed: {}", peer, error.getMessage());
          }
        });
  }

  /**
   * Handles a bootstrap request from a peer.
   * <p>
   * When handling a bootstrap request from a peer, the node sends batches of entries back to the peer and completes the
   * bootstrap request once all batches have been received and processed.
   *
   * @param peer the peer that sent the bootstrap request
   * @return a future to be completed once updates have been sent to the peer
   */
  private CompletableFuture<Void> handleBootstrap(MemberId peer) {
    LOGGER.trace("Received bootstrap request from {} for {}", peer, bootstrapMessageSubject);

    Function<List<UpdateEntry>, CompletableFuture<Void>> sendUpdates = updates -> {
      LOGGER.trace("Initializing {} with {} entries", peer, updates.size());
      return clusterCommunicator.<List<UpdateEntry>, Void>send(
          initializeMessageSubject,
          ImmutableList.copyOf(updates),
          serializer::encode,
          serializer::decode,
          peer)
          .whenComplete((result, error) -> {
            if (error != null) {
              LOGGER.debug("Failed to initialize {}", peer, error);
            }
          });
    };

    List<CompletableFuture<Void>> futures = Lists.newArrayList();
    List<UpdateEntry> updates = Lists.newArrayList();
    for (Map.Entry<String, MapValue> entry : items.entrySet()) {
      String key = entry.getKey();
      MapValue value = entry.getValue();
      if (value.isAlive()) {
        updates.add(new UpdateEntry(key, value));
        if (updates.size() == DEFAULT_MAX_EVENTS) {
          futures.add(sendUpdates.apply(updates));
          updates = new ArrayList<>();
        }
      }
    }

    if (!updates.isEmpty()) {
      futures.add(sendUpdates.apply(updates));
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  // TODO pull this into the class if this gets pulled out...
  private static final int DEFAULT_MAX_EVENTS = 1000;
  private static final int DEFAULT_MAX_IDLE_MS = 10;
  private static final int DEFAULT_MAX_BATCH_MS = 50;
  private static final Timer TIMER = new Timer("atomix-anti-entropy-map-sender-events");

  private final class EventAccumulator extends AbstractAccumulator<UpdateEntry> {

    private final MemberId peer;

    private EventAccumulator(MemberId peer) {
      super(TIMER, DEFAULT_MAX_EVENTS, DEFAULT_MAX_BATCH_MS, DEFAULT_MAX_IDLE_MS);
      this.peer = peer;
    }

    @Override
    public void processItems(List<UpdateEntry> items) {
      Map<String, UpdateEntry> map = Maps.newHashMap();
      items.forEach(item -> map.compute(item.key(), (key, existing) ->
          item.isNewerThan(existing) ? item : existing));
      communicationExecutor.execute(() -> {
        try {
          clusterCommunicator.unicast(
              updateMessageSubject,
              ImmutableList.copyOf(map.values()),
              serializer::encode,
              peer)
              .whenComplete((result, error) -> {
                if (error != null) {
                  LOGGER.debug("Failed to send to {}", peer, error);
                }
              });
        } catch (Exception e) {
          LOGGER.warn("Failed to send to {}", peer, e);
        }
      });
    }
  }
}
