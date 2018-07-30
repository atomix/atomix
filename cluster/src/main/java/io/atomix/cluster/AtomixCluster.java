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

import com.google.common.collect.Streams;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.MulticastDiscoveryConfig;
import io.atomix.cluster.discovery.MulticastDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryConfig;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.cluster.impl.DefaultClusterMembershipService;
import io.atomix.cluster.impl.DefaultNodeDiscoveryService;
import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.ManagedBroadcastService;
import io.atomix.cluster.messaging.ManagedClusterCommunicationService;
import io.atomix.cluster.messaging.ManagedClusterEventService;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.cluster.messaging.impl.DefaultClusterCommunicationService;
import io.atomix.cluster.messaging.impl.DefaultClusterEventService;
import io.atomix.cluster.messaging.impl.NettyBroadcastService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.config.ConfigMapper;
import io.atomix.utils.net.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomix cluster manager.
 * <p>
 * The cluster manager is the basis for all cluster management and communication in an Atomix cluster. This class is
 * responsible for bootstrapping new clusters or joining existing ones, establishing communication between nodes,
 * and detecting failures.
 * <p>
 * The Atomix cluster can be run as a standalone instance for cluster management and communication. To build a cluster
 * instance, use {@link #builder()} to create a new builder.
 * <pre>
 *   {@code
 *   AtomixCluster cluster = AtomixCluster.builder()
 *     .withClusterName("my-cluster")
 *     .withMemberId("member-1")
 *     .withAddress("localhost:1234")
 *     .withMulticastEnabled()
 *     .build();
 *   }
 * </pre>
 * The instance can be configured with a unique identifier via {@link AtomixClusterBuilder#withMemberId(String)}. The member ID
 * can be used to lookup the member in the {@link ClusterMembershipService} or send messages to this node from other
 * member nodes. The {@link AtomixClusterBuilder#withAddress(Address) address} is the host and port to which the node will bind
 * for intra-cluster communication over TCP.
 * <p>
 * Once an instance has been configured, the {@link #start()} method must be called to bootstrap the instance. The
 * {@code start()} method returns a {@link CompletableFuture} which will be completed once all the services have been
 * bootstrapped.
 * <pre>
 *   {@code
 *   cluster.start().join();
 *   }
 * </pre>
 * <p>
 * Cluster membership is determined by a configurable {@link NodeDiscoveryProvider}. To configure the membership
 * provider use {@link AtomixClusterBuilder#withMembershipProvider(NodeDiscoveryProvider)}. By default, the
 * {@link MulticastDiscoveryProvider} will be used if multicast is {@link AtomixClusterBuilder#withMulticastEnabled() enabled},
 * otherwise the {@link BootstrapDiscoveryProvider} will be used if no provider is explicitly provided.
 */
public class AtomixCluster implements BootstrapService, Managed<Void> {
  private static final String[] DEFAULT_RESOURCES = new String[]{"cluster"};

  private static String[] withDefaultResources(String config) {
    return Streams.concat(Stream.of(config), Stream.of(DEFAULT_RESOURCES)).toArray(String[]::new);
  }

  /**
   * Returns a new Atomix configuration from the given resources.
   *
   * @param resources   the resources from which to return a new Atomix configuration
   * @param classLoader the class loader
   * @return a new Atomix configuration from the given resource
   */
  private static ClusterConfig config(String[] resources, ClassLoader classLoader) {
    return new ConfigMapper(classLoader).loadResources(ClusterConfig.class, resources);
  }

  /**
   * Returns a new Atomix builder.
   *
   * @return a new Atomix builder
   */
  public static AtomixClusterBuilder builder() {
    return builder(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static AtomixClusterBuilder builder(ClassLoader classLoader) {
    return builder(config(DEFAULT_RESOURCES, classLoader));
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config the Atomix configuration
   * @return a new Atomix builder
   */
  public static AtomixClusterBuilder builder(String config) {
    return builder(config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config      the Atomix configuration
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static AtomixClusterBuilder builder(String config, ClassLoader classLoader) {
    return new AtomixClusterBuilder(config(withDefaultResources(config), classLoader));
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config the Atomix configuration
   * @return a new Atomix builder
   */
  public static AtomixClusterBuilder builder(ClusterConfig config) {
    return new AtomixClusterBuilder(config);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AtomixCluster.class);

  protected final ManagedMessagingService messagingService;
  protected final ManagedBroadcastService broadcastService;
  protected final NodeDiscoveryProvider discoveryProvider;
  protected final ManagedClusterMembershipService membershipService;
  protected final ManagedClusterCommunicationService communicationService;
  protected final ManagedClusterEventService eventService;
  protected volatile CompletableFuture<Void> openFuture;
  protected volatile CompletableFuture<Void> closeFuture;
  private final ThreadContext threadContext = new SingleThreadContext("atomix-cluster-%d");
  private final AtomicBoolean started = new AtomicBoolean();

  public AtomixCluster(String configFile) {
    this(loadConfig(new File(System.getProperty("user.dir"), configFile), Thread.currentThread().getContextClassLoader()));
  }

  public AtomixCluster(File configFile) {
    this(loadConfig(configFile, Thread.currentThread().getContextClassLoader()));
  }

  public AtomixCluster(ClusterConfig config) {
    this.messagingService = buildMessagingService(config);
    this.broadcastService = buildBroadcastService(config);
    this.discoveryProvider = buildLocationProvider(config);
    this.membershipService = buildClusterMembershipService(config, this, discoveryProvider);
    this.communicationService = buildClusterMessagingService(membershipService, messagingService);
    this.eventService = buildClusterEventService(membershipService, messagingService);
  }

  /**
   * Returns the cluster broadcast service.
   * <p>
   * The broadcast service is used to broadcast messages to all nodes in the cluster via multicast.
   * The broadcast service is disabled by default. To enable broadcast, the cluster must be configured with
   * {@link AtomixClusterBuilder#withMulticastEnabled() multicast enabled}.
   *
   * @return the cluster broadcast service
   */
  @Override
  public BroadcastService getBroadcastService() {
    return broadcastService;
  }

  /**
   * Returns the cluster messaging service.
   * <p>
   * The messaging service is used for direct point-to-point messaging between nodes by {@link Address}. This is a
   * low-level cluster communication API. For higher level messaging, use the
   * {@link #getCommunicationService() communication service} or {@link #getEventService() event service}.
   *
   * @return the cluster messaging service
   */
  @Override
  public MessagingService getMessagingService() {
    return messagingService;
  }

  /**
   * Returns the cluster membership service.
   * <p>
   * The membership service manages cluster membership information and failure detection.
   *
   * @return the cluster membership service
   */
  public ClusterMembershipService getMembershipService() {
    return membershipService;
  }

  /**
   * Returns the cluster communication service.
   * <p>
   * The cluster communication service is used for high-level unicast, multicast, broadcast, and request-reply messaging.
   *
   * @return the cluster communication service
   */
  public ClusterCommunicationService getCommunicationService() {
    return communicationService;
  }

  /**
   * Returns the cluster event service.
   * <p>
   * The cluster event service is used for high-level publish-subscribe messaging.
   *
   * @return the cluster event service
   */
  public ClusterEventService getEventService() {
    return eventService;
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<Void> start() {
    if (closeFuture != null) {
      return Futures.exceptionalFuture(new IllegalStateException("AtomixCluster instance " +
          (closeFuture.isDone() ? "shutdown" : "shutting down")));
    }

    if (openFuture != null) {
      return openFuture;
    }

    openFuture = startServices()
        .thenComposeAsync(v -> completeStartup(), threadContext);

    return openFuture;
  }

  protected CompletableFuture<Void> startServices() {
    return messagingService.start()
        .thenComposeAsync(v -> broadcastService.start(), threadContext)
        .thenComposeAsync(v -> membershipService.start(), threadContext)
        .thenComposeAsync(v -> communicationService.start(), threadContext)
        .thenComposeAsync(v -> eventService.start(), threadContext)
        .thenApply(v -> null);
  }

  protected CompletableFuture<Void> completeStartup() {
    started.set(true);
    LOGGER.info("Started");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public synchronized CompletableFuture<Void> stop() {
    if (closeFuture != null) {
      return closeFuture;
    }

    closeFuture = stopServices()
        .thenComposeAsync(v -> completeShutdown(), threadContext);
    return closeFuture;
  }

  protected CompletableFuture<Void> stopServices() {
    return communicationService.stop()
        .exceptionally(e -> null)
        .thenComposeAsync(v -> eventService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> membershipService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> broadcastService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> messagingService.stop(), threadContext)
        .exceptionally(e -> null);
  }

  protected CompletableFuture<Void> completeShutdown() {
    threadContext.close();
    started.set(false);
    LOGGER.info("Stopped");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .toString();
  }

  /**
   * Loads a configuration from the given file.
   */
  private static ClusterConfig loadConfig(File config, ClassLoader classLoader) {
    return new ConfigMapper(classLoader).loadResources(ClusterConfig.class, config.getAbsolutePath());
  }

  /**
   * Builds a default messaging service.
   */
  protected static ManagedMessagingService buildMessagingService(ClusterConfig config) {
    return NettyMessagingService.builder()
        .withName(config.getClusterId())
        .withAddress(config.getNodeConfig().getAddress())
        .build();
  }

  /**
   * Builds a default broadcast service.
   */
  protected static ManagedBroadcastService buildBroadcastService(ClusterConfig config) {
    return NettyBroadcastService.builder()
        .withLocalAddress(config.getNodeConfig().getAddress())
        .withGroupAddress(new Address(
            config.getMulticastConfig().getGroup().getHostAddress(),
            config.getMulticastConfig().getPort(),
            config.getMulticastConfig().getGroup()))
        .withEnabled(config.getMulticastConfig().isEnabled())
        .build();
  }

  /**
   * Builds a member location provider.
   */
  @SuppressWarnings("unchecked")
  protected static NodeDiscoveryProvider buildLocationProvider(ClusterConfig config) {
    NodeDiscoveryConfig discoveryProviderConfig = config.getDiscoveryConfig();
    if (discoveryProviderConfig != null) {
      return discoveryProviderConfig.getType().newProvider(discoveryProviderConfig);
    }
    if (config.getMulticastConfig().isEnabled()) {
      return new MulticastDiscoveryProvider(new MulticastDiscoveryConfig());
    } else {
      return new BootstrapDiscoveryProvider(Collections.emptyList());
    }
  }

  /**
   * Builds a cluster service.
   */
  protected static ManagedClusterMembershipService buildClusterMembershipService(
      ClusterConfig config,
      BootstrapService bootstrapService,
      NodeDiscoveryProvider discoveryProvider) {
    // If the local node has not be configured, create a default node.
    Member localMember = Member.builder()
        .withId(config.getNodeConfig().getId())
        .withAddress(config.getNodeConfig().getAddress())
        .withHost(config.getNodeConfig().getHost())
        .withRack(config.getNodeConfig().getRack())
        .withZone(config.getNodeConfig().getZone())
        .withProperties(config.getNodeConfig().getProperties())
        .build();
    return new DefaultClusterMembershipService(
        localMember,
        new DefaultNodeDiscoveryService(bootstrapService, localMember, discoveryProvider),
        bootstrapService,
        config.getMembershipConfig());
  }

  /**
   * Builds a cluster messaging service.
   */
  protected static ManagedClusterCommunicationService buildClusterMessagingService(
      ClusterMembershipService membershipService, MessagingService messagingService) {
    return new DefaultClusterCommunicationService(membershipService, messagingService);
  }

  /**
   * Builds a cluster event service.
   */
  protected static ManagedClusterEventService buildClusterEventService(
      ClusterMembershipService membershipService, MessagingService messagingService) {
    return new DefaultClusterEventService(membershipService, messagingService);
  }
}
