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
 * Cluster membership provider that bootstraps membership from a pre-defined set of peers.
 * <p>
 * The bootstrap member provider takes a set of peer {@link Config#setLocations(Collection) addresses} and uses them
 * to join the cluster. Using the {@link io.atomix.cluster.messaging.MessagingService}, each node sends a heartbeat to
 * its configured bootstrap peers. Peers respond to each heartbeat message with a list of all known peers, thus
 * propagating membership information using a gossip style protocol.
 * <p>
 * A phi accrual failure detector is used to detect failures and remove peers from the configuration. In order to avoid
 * flapping of membership following a {@link ClusterMembershipEvent.Type#MEMBER_ADDED} event, the implementation attempts
 * to heartbeat all newly discovered peers before triggering a {@link ClusterMembershipEvent.Type#MEMBER_REMOVED} event.
 */
public class BootstrapDiscoveryProvider
    extends AbstractListenerManager<NodeDiscoveryEvent, NodeDiscoveryEventListener>
    implements NodeDiscoveryProvider {

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
  public static class Type implements NodeDiscoveryProvider.Type<Config> {
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
    public NodeDiscoveryProvider newProvider(Config config) {
      return new BootstrapDiscoveryProvider(config);
    }
  }

  /**
   * Bootstrap member location provider builder.
   */
  public static class Builder implements NodeDiscoveryProvider.Builder {
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
    public NodeDiscoveryProvider build() {
      return new BootstrapDiscoveryProvider(config);
    }
  }

  /**
   * Bootstrap location provider configuration.
   */
  public static class Config implements NodeDiscoveryProvider.Config {
    private static final int DEFAULT_HEARTBEAT_INTERVAL = 100;
    private static final int DEFAULT_FAILURE_TIMEOUT = 10000;
    private static final int DEFAULT_PHI_FAILURE_THRESHOLD = 10;

    private int heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private int failureThreshold = DEFAULT_PHI_FAILURE_THRESHOLD;
    private int failureTimeout = DEFAULT_FAILURE_TIMEOUT;
    private Collection<Address> locations = Collections.emptySet();

    @Override
    public NodeDiscoveryProvider.Type getType() {
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

  private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapDiscoveryProvider.class);
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
      .register(Node.class)
      .register(NodeId.class)
      .register(NodeId.Type.class)
      .register(new AddressSerializer(), Address.class)
      .build());

  private static final String HEARTBEAT_MESSAGE = "atomix-cluster-heartbeat";

  private final Collection<Address> bootstrapLocations;
  private final Config config;

  private volatile BootstrapService bootstrap;

  private Map<Address, Node> nodes = Maps.newConcurrentMap();

  private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
      namedThreads("atomix-cluster-heartbeat-sender", LOGGER));
  private final ExecutorService heartbeatExecutor = Executors.newSingleThreadExecutor(
      namedThreads("atomix-cluster-heartbeat-receiver", LOGGER));
  private ScheduledFuture<?> heartbeatFuture;

  private final Map<Address, PhiAccrualFailureDetector> failureDetectors = Maps.newConcurrentMap();

  public BootstrapDiscoveryProvider(Address... bootstrapLocations) {
    this(Arrays.asList(bootstrapLocations));
  }

  public BootstrapDiscoveryProvider(Collection<Address> bootstrapLocations) {
    this(new Config().setLocations(bootstrapLocations));
  }

  BootstrapDiscoveryProvider(Config config) {
    this.config = checkNotNull(config);
    this.bootstrapLocations = ImmutableSet.copyOf(config.getLocations());
  }

  @Override
  public NodeDiscoveryProvider.Config config() {
    return config;
  }

  @Override
  public Set<Node> getNodes() {
    return ImmutableSet.copyOf(nodes.values());
  }

  /**
   * Sends heartbeats to all peers.
   */
  private CompletableFuture<Void> sendHeartbeats(Node localNode) {
    Stream<Address> clusterLocations = this.nodes.values().stream()
        .filter(node -> !node.address().equals(localNode.address()))
        .map(node -> node.address());

    Stream<Address> bootstrapLocations = this.bootstrapLocations.stream()
        .filter(location -> !location.equals(localNode.address()));

    return Futures.allOf(Stream.concat(clusterLocations, bootstrapLocations).map(address -> {
      LOGGER.trace("{} - Sending heartbeat: {}", localNode.address(), address);
      return sendHeartbeat(localNode, address).exceptionally(v -> null);
    }).collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  /**
   * Sends a heartbeat to the given peer.
   */
  private CompletableFuture<Void> sendHeartbeat(Node localNode, Address address) {
    return bootstrap.getMessagingService().sendAndReceive(address, HEARTBEAT_MESSAGE, SERIALIZER.encode(localNode)).whenComplete((response, error) -> {
      if (error == null) {
        Collection<Node> nodes = SERIALIZER.decode(response);
        for (Node node : nodes) {
          if (node.address().equals(address)) {
            Node oldNode = this.nodes.put(node.address(), node);
            if (oldNode != null && !oldNode.id().equals(node.id())) {
              failureDetectors.remove(oldNode.address());
              post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.LEAVE, oldNode));
              post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.JOIN, node));
            } else if (oldNode == null) {
              post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.JOIN, node));
            }
          } else if (!this.nodes.containsKey(node.address()) || !this.nodes.get(node.address()).id().equals(node.id())) {
            sendHeartbeat(localNode, node.address());
          }
        }
      } else {
        LOGGER.debug("{} - Sending heartbeat to {} failed", localNode, address, error);
        PhiAccrualFailureDetector failureDetector = failureDetectors.computeIfAbsent(address, n -> new PhiAccrualFailureDetector());
        double phi = failureDetector.phi();
        if (phi >= config.getFailureThreshold()
            || (phi == 0.0 && System.currentTimeMillis() - failureDetector.lastUpdated() > config.getFailureTimeout())) {
          Node node = this.nodes.remove(address);
          if (node != null) {
            failureDetectors.remove(node.address());
            post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.LEAVE, node));
          }
        }
      }
    }).exceptionally(e -> null)
        .thenApply(v -> null);
  }

  /**
   * Handles a heartbeat message.
   */
  private byte[] handleHeartbeat(Node localNode, Node node) {
    LOGGER.trace("{} - Received heartbeat: {}", localNode.address(), localNode.address());
    failureDetectors.computeIfAbsent(localNode.address(), n -> new PhiAccrualFailureDetector()).report();
    Node oldNode = nodes.put(node.address(), node);
    if (oldNode != null && !oldNode.id().equals(node.id())) {
      failureDetectors.remove(oldNode.address());
      post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.LEAVE, oldNode));
      post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.JOIN, node));
    } else if (oldNode == null) {
      post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.JOIN, node));
    }
    return SERIALIZER.encode(Lists.newArrayList(nodes.values()));
  }

  @Override
  public CompletableFuture<Void> join(BootstrapService bootstrap, Node localNode) {
    if (nodes.putIfAbsent(localNode.address(), localNode) == null) {
      this.bootstrap = bootstrap;
      post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.JOIN, localNode));

      bootstrap.getMessagingService().registerHandler(
          HEARTBEAT_MESSAGE,
          (BiFunction<Address, byte[], byte[]>) (a, p) ->
              handleHeartbeat(localNode, SERIALIZER.decode(p)), heartbeatExecutor);

      ComposableFuture<Void> future = new ComposableFuture<>();
      sendHeartbeats(localNode).whenComplete((r, e) -> {
        future.complete(null);
      });

      heartbeatFuture = heartbeatScheduler.scheduleWithFixedDelay(() -> {
        sendHeartbeats(localNode);
      }, 0, config.getHeartbeatInterval(), TimeUnit.MILLISECONDS);

      return future.thenRun(() -> {
        LOGGER.info("Joined");
      });
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> leave(Node localNode) {
    if (nodes.remove(localNode.address()) != null) {
      post(new NodeDiscoveryEvent(NodeDiscoveryEvent.Type.LEAVE, localNode));

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
