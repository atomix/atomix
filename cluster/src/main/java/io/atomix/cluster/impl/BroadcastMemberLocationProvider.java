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
package io.atomix.cluster.impl;

import com.google.common.collect.Sets;
import io.atomix.cluster.MemberLocationEvent;
import io.atomix.cluster.MemberLocationEventListener;
import io.atomix.cluster.MemberLocationProvider;
import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Member location provider implementation that uses the broadcast service.
 */
public class BroadcastMemberLocationProvider
    extends AbstractListenerManager<MemberLocationEvent, MemberLocationEventListener>
    implements MemberLocationProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(BroadcastMemberLocationProvider.class);
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
      .register(new AddressSerializer(), Address.class)
      .build());

  private final BroadcastService broadcastService;
  private final boolean enabled;
  private final ScheduledExecutorService broadcastScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-broadcast", LOGGER));
  private final Duration broadcastInterval = Duration.ofSeconds(5);
  private volatile ScheduledFuture<?> broadcastFuture;
  private final Consumer<byte[]> broadcastListener = this::handleBroadcastMessage;
  private final Set<Address> locations = Sets.newConcurrentHashSet();

  public BroadcastMemberLocationProvider(BroadcastService broadcastService, boolean enabled) {
    this.broadcastService = checkNotNull(broadcastService);
    this.enabled = enabled;
  }

  private void handleBroadcastMessage(byte[] message) {
    Address address = SERIALIZER.decode(message);
    if (locations.add(address)) {
      post(new MemberLocationEvent(MemberLocationEvent.Type.JOIN, address));
    }
  }

  private void broadcastLocation(Address address) {
    broadcastService.broadcast(SERIALIZER.encode(address));
  }

  @Override
  public CompletableFuture<Void> join(Address address) {
    if (!enabled) {
      return CompletableFuture.completedFuture(null);
    }

    locations.add(address);
    broadcastService.addListener(broadcastListener);
    broadcastFuture = broadcastScheduler.scheduleWithFixedDelay(
        () -> broadcastLocation(address),
        broadcastInterval.toMillis(),
        broadcastInterval.toMillis(),
        TimeUnit.MILLISECONDS);
    broadcastLocation(address);
    LOGGER.info("Joined");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> leave() {
    if (!enabled) {
      return CompletableFuture.completedFuture(null);
    }

    broadcastService.removeListener(broadcastListener);
    ScheduledFuture<?> broadcastFuture = this.broadcastFuture;
    if (broadcastFuture != null) {
      broadcastFuture.cancel(false);
    }
    broadcastScheduler.shutdownNow();
    return CompletableFuture.completedFuture(null);
  }
}
