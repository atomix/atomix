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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.impl.AddressSerializer;
import io.atomix.cluster.impl.PhiAccrualFailureDetector;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.event.AbstractListenerManager;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Bootstrap member location provider.
 */
public class BootstrapMembershipProvider
    extends AbstractListenerManager<MemberLocationEvent, MemberLocationEventListener>
    implements ClusterMembershipProvider {

  private static final Type TYPE = new Type();

  /**
   * Creates a new bootstrap provider builder.
   *
   * @return a new bootstrap provider builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Bootstrap member location provider type.
   */
  public static class Type implements ClusterMembershipProvider.Type<Config> {
    private static final String NAME = "bootstrap";

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
      return new BootstrapMembershipProvider(config);
    }
  }

  /**
   * Bootstrap member location provider builder.
   */
  public static class Builder implements ClusterMembershipProvider.Builder {
    private final Config config = new Config();

    /**
     * Sets the bootstrap member locations.
     *
     * @param locations the bootstrap member locations
     * @return the location provider builder
     */
    public Builder withLocations(Address... locations) {
      return withLocations(Arrays.asList(locations));
    }

    /**
     * Sets the bootstrap member locations.
     *
     * @param locations the bootstrap member locations
     * @return the location provider builder
     */
    public Builder withLocations(Collection<Address> locations) {
      config.setLocations(locations);
      return this;
    }

    /**
     * Sets the failure detection heartbeat interval.
     *
     * @param heartbeatInterval the failure detection heartbeat interval
     * @return the location provider builder
     */
    public Builder withHeartbeatInterval(Duration heartbeatInterval) {
      config.setHeartbeatInterval((int) heartbeatInterval.toMillis());
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
      return new BootstrapMembershipProvider(config);
    }
  }

  /**
   * Bootstrap location provider configuration.
   */
  public static class Config implements ClusterMembershipProvider.Config {
    private static final int DEFAULT_HEARTBEAT_INTERVAL = 100;
    private static final int DEFAULT_FAILURE_TIMEOUT = 10000;
    private static final int DEFAULT_PHI_FAILURE_THRESHOLD = 10;

    private int heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private int failureThreshold = DEFAULT_PHI_FAILURE_THRESHOLD;
    private int failureTimeout = DEFAULT_FAILURE_TIMEOUT;
    private Collection<Address> locations = Collections.emptySet();

    @Override
    public ClusterMembershipProvider.Type getType() {
      return TYPE;
    }

    /**
     * Returns the configured bootstrap locations.
     *
     * @return the configured bootstrap locations
     */
    public Collection<Address> getLocations() {
      return locations;
    }

    /**
     * Sets the bootstrap locations.
     *
     * @param locations the bootstrap locations
     * @return the bootstrap provider configuration
     */
    public Config setLocations(Collection<Address> locations) {
      this.locations = locations;
      return this;
    }

    /**
     * Returns the heartbeat interval.
     *
     * @return the heartbeat interval
     */
    public int getHeartbeatInterval() {
      return heartbeatInterval;
    }

    /**
     * Sets the heartbeat interval.
     *
     * @param heartbeatInterval the heartbeat interval
     * @return the group membership configuration
     */
    public Config setHeartbeatInterval(int heartbeatInterval) {
      this.heartbeatInterval = heartbeatInterval;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapMembershipProvider.class);
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
      .register(new AddressSerializer(), Address.class)
      .build());

  private static final String HEARTBEAT_MESSAGE = "atomix-cluster-heartbeat";
  private static final byte[] EMPTY_PAYLOAD = new byte[0];

  private final Collection<Address> bootstrapLocations;
  private final Config config;

  private volatile BootstrapService bootstrap;

  private Set<Address> locations = Sets.newCopyOnWriteArraySet();

  private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-heartbeat-sender", LOGGER));
  private final ExecutorService heartbeatExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-cluster-heartbeat-receiver", LOGGER));
  private ScheduledFuture<?> heartbeatFuture;

  private final Map<Address, PhiAccrualFailureDetector> failureDetectors = Maps.newConcurrentMap();

  public BootstrapMembershipProvider(Address... bootstrapLocations) {
    this(Arrays.asList(bootstrapLocations));
  }

  public BootstrapMembershipProvider(Collection<Address> bootstrapLocations) {
    this(new Config().setLocations(bootstrapLocations));
  }

  BootstrapMembershipProvider(Config config) {
    this.config = checkNotNull(config);
    this.bootstrapLocations = ImmutableSet.copyOf(config.getLocations());
  }

  @Override
  public ClusterMembershipProvider.Config config() {
    return config;
  }

  @Override
  public Set<Address> getLocations() {
    return ImmutableSet.copyOf(locations);
  }

  /**
   * Sends heartbeats to all peers.
   */
  private CompletableFuture<Void> sendHeartbeats(Address localAddress) {
    Stream<Address> clusterLocations = this.locations.stream()
        .filter(location -> !location.equals(localAddress));

    Stream<Address> bootstrapLocations = this.bootstrapLocations.stream()
        .filter(location -> !location.equals(localAddress));

    return Futures.allOf(Stream.concat(clusterLocations, bootstrapLocations).map(address -> {
      LOGGER.trace("{} - Sending heartbeat: {}", localAddress, address);
      return sendHeartbeat(localAddress, address).exceptionally(v -> null);
    }).collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  /**
   * Sends a heartbeat to the given peer.
   */
  private CompletableFuture<Void> sendHeartbeat(Address localAddress, Address address) {
    return bootstrap.getMessagingService().sendAndReceive(address, HEARTBEAT_MESSAGE, EMPTY_PAYLOAD).whenComplete((response, error) -> {
      if (error == null) {
        Collection<Address> locations = SERIALIZER.decode(response);
        for (Address location : locations) {
          if (this.locations.add(location)) {
            post(new MemberLocationEvent(MemberLocationEvent.Type.JOIN, location));
          }
        }
      } else {
        LOGGER.debug("{} - Sending heartbeat to {} failed", localAddress, address, error);
        PhiAccrualFailureDetector failureDetector = failureDetectors.computeIfAbsent(address, n -> new PhiAccrualFailureDetector());
        double phi = failureDetector.phi();
        if (phi >= config.getFailureThreshold()
            || (phi == 0.0 && failureDetector.lastUpdated() > 0
            && System.currentTimeMillis() - failureDetector.lastUpdated() > config.getFailureTimeout())) {
          if (this.locations.remove(address)) {
            post(new MemberLocationEvent(MemberLocationEvent.Type.LEAVE, address));
          }
        }
      }
    }).exceptionally(e -> null)
        .thenApply(v -> null);
  }

  /**
   * Handles a heartbeat message.
   */
  private byte[] handleHeartbeat(Address localAddress, Address address) {
    LOGGER.trace("{} - Received heartbeat: {}", localAddress, address);
    failureDetectors.computeIfAbsent(address, n -> new PhiAccrualFailureDetector()).report();
    if (locations.add(address)) {
      post(new MemberLocationEvent(MemberLocationEvent.Type.JOIN, address));
    }
    return SERIALIZER.encode(Lists.newArrayList(locations));
  }

  @Override
  public CompletableFuture<Void> join(BootstrapService bootstrap, Address address) {
    if (locations.add(address)) {
      this.bootstrap = bootstrap;
      post(new MemberLocationEvent(MemberLocationEvent.Type.JOIN, address));

      bootstrap.getMessagingService().registerHandler(
          HEARTBEAT_MESSAGE,
          (BiFunction<Address, byte[], byte[]>) (a, p) -> handleHeartbeat(address, a), heartbeatExecutor);

      ComposableFuture<Void> future = new ComposableFuture<>();
      sendHeartbeats(address).whenComplete((r, e) -> {
        future.complete(null);
      });

      heartbeatFuture = heartbeatScheduler.scheduleWithFixedDelay(() -> {
        sendHeartbeats(address);
      }, 0, config.getHeartbeatInterval(), TimeUnit.MILLISECONDS);

      return future.thenRun(() -> {
        LOGGER.info("Joined");
      });
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> leave(Address address) {
    if (locations.remove(address)) {
      post(new MemberLocationEvent(MemberLocationEvent.Type.LEAVE, address));

      bootstrap.getMessagingService().unregisterHandler(HEARTBEAT_MESSAGE);
      ScheduledFuture<?> heartbeatFuture = this.heartbeatFuture;
      if (heartbeatFuture != null) {
        heartbeatFuture.cancel(false);
      }
      heartbeatExecutor.shutdownNow();
      LOGGER.info("Left");
    }
    return CompletableFuture.completedFuture(null);
  }
}
