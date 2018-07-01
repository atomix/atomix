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
package io.atomix.cluster;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.impl.AddressSerializer;
import io.atomix.cluster.impl.PhiAccrualFailureDetector;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
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
 * Cluster membership provider that uses multicast for member discovery.
 * <p>
 * This implementation uses the {@link io.atomix.cluster.messaging.BroadcastService} internally and thus requires
 * that multicast is {@link AtomixCluster.Builder#withMulticastEnabled() enabled} on the Atomix instance. Membership
 * is determined by each node broadcasting to a multicast group, and phi accrual failure detectors are used to detect
 * nodes joining and leaving the cluster.
 */
public class MulticastMembershipProvider
    extends AbstractListenerManager<MemberLocationEvent, MemberLocationEventListener>
    implements ClusterMembershipProvider {

  private static final Type TYPE = new Type();

  /**
   * Returns a new multicast member location provider builder.
   *
   * @return a new multicast location provider builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Broadcast member location provider type.
   */
  public static class Type implements ClusterMembershipProvider.Type<Config> {
    private static final String NAME = "multicast";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Config newConfig() {
      return new Config();
    }

    @Override
    public ClusterMembershipProvider newProvider(Config config) {
      return new MulticastMembershipProvider(config);
    }
  }

  /**
   * Multicast member location provider builder.
   */
  public static class Builder implements ClusterMembershipProvider.Builder {
    private final Config config = new Config();

    private Builder() {
    }

    /**
     * Sets the broadcast interval.
     *
     * @param broadcastInterval the broadcast interval
     * @return the location provider builder
     */
    public Builder withBroadcastInterval(Duration broadcastInterval) {
      config.setBroadcastInterval((int) broadcastInterval.toMillis());
      return this;
    }

    /**
     * Sets the phi accrual failure threshold.
     *
     * @param failureThreshold the phi accrual failure threshold
     * @return the location provider builder
     */
    public Builder withFailureThreshold(int failureThreshold) {
      config.setFailureThreshold(failureThreshold);
      return this;
    }

    /**
     * Sets the failure timeout to use prior to phi failure detectors being populated.
     *
     * @param failureTimeout the failure timeout
     * @return the location provider builder
     */
    public Builder withFailureTimeout(Duration failureTimeout) {
      config.setFailureTimeout((int) failureTimeout.toMillis());
      return this;
    }

    @Override
    public ClusterMembershipProvider build() {
      return new MulticastMembershipProvider(config);
    }
  }

  /**
   * Member location provider configuration.
   */
  public static class Config implements ClusterMembershipProvider.Config {
    private static final int DEFAULT_BROADCAST_INTERVAL = 100;
    private static final int DEFAULT_FAILURE_TIMEOUT = 10000;
    private static final int DEFAULT_PHI_FAILURE_THRESHOLD = 10;

    private int broadcastInterval = DEFAULT_BROADCAST_INTERVAL;
    private int failureThreshold = DEFAULT_PHI_FAILURE_THRESHOLD;
    private int failureTimeout = DEFAULT_FAILURE_TIMEOUT;

    @Override
    public ClusterMembershipProvider.Type getType() {
      return TYPE;
    }

    /**
     * Returns the broadcast interval.
     *
     * @return the broadcast interval
     */
    public int getBroadcastInterval() {
      return broadcastInterval;
    }

    /**
     * Sets the broadcast interval.
     *
     * @param broadcastInterval the broadcast interval
     * @return the group membership configuration
     */
    public Config setBroadcastInterval(int broadcastInterval) {
      this.broadcastInterval = broadcastInterval;
      return this;
    }

    /**
     * Returns the failure detector threshold.
     *
     * @return the failure detector threshold
     */
    public int getFailureThreshold() {
      return failureThreshold;
    }

    /**
     * Sets the failure detector threshold.
     *
     * @param failureThreshold the failure detector threshold
     * @return the group membership configuration
     */
    public Config setFailureThreshold(int failureThreshold) {
      this.failureThreshold = failureThreshold;
      return this;
    }

    /**
     * Returns the base failure timeout.
     *
     * @return the base failure timeout
     */
    public int getFailureTimeout() {
      return failureTimeout;
    }

    /**
     * Sets the base failure timeout.
     *
     * @param failureTimeout the base failure timeout
     * @return the group membership configuration
     */
    public Config setFailureTimeout(int failureTimeout) {
      this.failureTimeout = failureTimeout;
      return this;
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(MulticastMembershipProvider.class);
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
      .register(new AddressSerializer(), Address.class)
      .build());

  private final Config config;
  private volatile BootstrapService bootstrap;

  private final ScheduledExecutorService broadcastScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-broadcast", LOGGER));
  private final Duration broadcastInterval = Duration.ofSeconds(5);
  private volatile ScheduledFuture<?> broadcastFuture;
  private final Consumer<byte[]> broadcastListener = this::handleBroadcastMessage;

  // A set of active peer locations.
  private final Set<Address> locations = Sets.newConcurrentHashSet();

  // A map of failure detectors, one for each active peer.
  private final Map<Address, PhiAccrualFailureDetector> failureDetectors = Maps.newConcurrentMap();
  private volatile ScheduledFuture<?> failureFuture;

  public MulticastMembershipProvider() {
    this(new Config());
  }

  MulticastMembershipProvider(Config config) {
    this.config = checkNotNull(config);
  }

  @Override
  public ClusterMembershipProvider.Config config() {
    return config;
  }

  @Override
  public Set<Address> getLocations() {
    return ImmutableSet.copyOf(locations);
  }

  private void handleBroadcastMessage(byte[] message) {
    Address address = SERIALIZER.decode(message);
    if (locations.add(address)) {
      LOGGER.info("Found peer at {}", address);
      post(new MemberLocationEvent(MemberLocationEvent.Type.JOIN, address));
    }
  }

  private void broadcastLocation(Address address) {
    bootstrap.getBroadcastService().broadcast(SERIALIZER.encode(address));
  }

  private void detectFailures(Address address) {
    locations.stream().filter(location -> !location.equals(address)).forEach(this::detectFailure);
  }

  private void detectFailure(Address address) {
    PhiAccrualFailureDetector failureDetector = failureDetectors.computeIfAbsent(address, n -> new PhiAccrualFailureDetector());
    double phi = failureDetector.phi();
    if (phi >= config.getFailureThreshold()
        || (phi == 0.0 && failureDetector.lastUpdated() > 0
        && System.currentTimeMillis() - failureDetector.lastUpdated() > config.getFailureTimeout())) {
      LOGGER.info("Lost contact with {}", address);
      locations.remove(address);
      failureDetectors.remove(address);
      post(new MemberLocationEvent(MemberLocationEvent.Type.LEAVE, address));
    }
  }

  @Override
  public CompletableFuture<Void> join(BootstrapService bootstrap, Address address) {
    if (locations.add(address)) {
      this.bootstrap = bootstrap;
      post(new MemberLocationEvent(MemberLocationEvent.Type.JOIN, address));
      bootstrap.getBroadcastService().addListener(broadcastListener);
      broadcastFuture = broadcastScheduler.scheduleAtFixedRate(
          () -> broadcastLocation(address),
          broadcastInterval.toMillis(),
          broadcastInterval.toMillis(),
          TimeUnit.MILLISECONDS);
      failureFuture = broadcastScheduler.scheduleAtFixedRate(
          () -> detectFailures(address),
          config.getBroadcastInterval() / 2,
          config.getBroadcastInterval() / 2,
          TimeUnit.MILLISECONDS);
      broadcastLocation(address);
      LOGGER.info("Joined");
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> leave(Address address) {
    if (locations.remove(address)) {
      post(new MemberLocationEvent(MemberLocationEvent.Type.LEAVE, address));
      bootstrap.getBroadcastService().removeListener(broadcastListener);
      ScheduledFuture<?> broadcastFuture = this.broadcastFuture;
      if (broadcastFuture != null) {
        broadcastFuture.cancel(false);
      }
      ScheduledFuture<?> failureFuture = this.failureFuture;
      if (failureFuture != null) {
        failureFuture.cancel(false);
      }
      broadcastScheduler.shutdownNow();
      LOGGER.info("Left");
    }
    return CompletableFuture.completedFuture(null);
  }
}
