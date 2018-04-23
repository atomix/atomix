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

import io.atomix.cluster.impl.DefaultBootstrapMetadataService;
import io.atomix.cluster.impl.DefaultClusterService;
import io.atomix.cluster.impl.DefaultPersistentMetadataService;
import io.atomix.cluster.messaging.ClusterEventingService;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.cluster.messaging.ManagedClusterEventingService;
import io.atomix.cluster.messaging.ManagedClusterMessagingService;
import io.atomix.cluster.messaging.impl.DefaultClusterEventingService;
import io.atomix.cluster.messaging.impl.DefaultClusterMessagingService;
import io.atomix.messaging.BroadcastService;
import io.atomix.messaging.ManagedBroadcastService;
import io.atomix.messaging.ManagedMessagingService;
import io.atomix.messaging.MessagingService;
import io.atomix.messaging.impl.NettyBroadcastService;
import io.atomix.messaging.impl.NettyMessagingService;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.config.Configs;
import io.atomix.utils.net.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster configuration.
 */
public class AtomixCluster<T extends AtomixCluster<T>> implements Managed<T> {

  /**
   * Returns a new Atomix cluster builder.
   *
   * @return a new Atomix cluster builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a new Atomix cluster builder.
   *
   * @param configFile the configuration file with which to initialize the builder
   * @return a new Atomix cluster builder
   */
  public static Builder builder(File configFile) {
    return new Builder(loadConfig(configFile));
  }

  /**
   * Returns a new Atomix cluster builder.
   *
   * @param config the Atomix cluster configuration
   * @return a new Atomix cluster builder
   */
  public static Builder builder(ClusterConfig config) {
    return new Builder(config);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AtomixCluster.class);

  protected final ManagedMessagingService messagingService;
  protected final ManagedBroadcastService broadcastService;
  protected final ManagedBootstrapMetadataService bootstrapMetadataService;
  protected final ManagedPersistentMetadataService persistentMetadataService;
  protected final ManagedClusterService clusterService;
  protected final ManagedClusterMessagingService clusterMessagingService;
  protected final ManagedClusterEventingService clusterEventingService;
  protected volatile CompletableFuture openFuture;
  protected volatile CompletableFuture<Void> closeFuture;
  private final ThreadContext threadContext = new SingleThreadContext("atomix-cluster-%d");
  private final AtomicBoolean started = new AtomicBoolean();

  public AtomixCluster(String configFile) {
    this(loadConfig(new File(System.getProperty("user.dir"), configFile)));
  }

  public AtomixCluster(File configFile) {
    this(loadConfig(configFile));
  }

  public AtomixCluster(ClusterConfig config) {
    // Apply profiles to all configurations.
    config.getProfile().configure(config);
    config.getLocalNode().getProfile().configure(config.getLocalNode());
    config.getNodes().forEach(node -> node.getProfile().configure(node));

    this.messagingService = buildMessagingService(config);
    this.broadcastService = buildBroadcastService(config);
    this.bootstrapMetadataService = buildBootstrapMetadataService(config);
    this.persistentMetadataService = buildPersistentMetadataService(config, messagingService);
    this.clusterService = buildClusterService(config, bootstrapMetadataService, persistentMetadataService, messagingService, broadcastService);
    this.clusterMessagingService = buildClusterMessagingService(clusterService, messagingService);
    this.clusterEventingService = buildClusterEventService(clusterService, messagingService);
  }

  /**
   * Returns the cluster service.
   *
   * @return the cluster service
   */
  public ClusterService clusterService() {
    return clusterService;
  }

  /**
   * Returns the cluster messaging service.
   *
   * @return the cluster messaging service
   */
  public ClusterMessagingService messagingService() {
    return clusterMessagingService;
  }

  /**
   * Returns the cluster event service.
   *
   * @return the cluster event service
   */
  public ClusterEventingService eventingService() {
    return clusterEventingService;
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<T> start() {
    if (closeFuture != null) {
      return Futures.exceptionalFuture(new IllegalStateException("AtomixCluster instance " +
          (closeFuture.isDone() ? "shutdown" : "shutting down")));
    }

    if (openFuture != null) {
      return openFuture;
    }

    openFuture = startServices()
        .thenComposeAsync(v -> joinCluster(), threadContext)
        .thenComposeAsync(v -> completeStartup(), threadContext)
        .thenApply(v -> this);

    return openFuture;
  }

  protected CompletableFuture<Void> startServices() {
    return messagingService.start()
        .thenComposeAsync(v -> broadcastService.start(), threadContext)
        .thenComposeAsync(v -> bootstrapMetadataService.start(), threadContext)
        .thenComposeAsync(v -> persistentMetadataService.start(), threadContext)
        .thenComposeAsync(v -> clusterService.start(), threadContext)
        .thenComposeAsync(v -> clusterMessagingService.start(), threadContext)
        .thenComposeAsync(v -> clusterEventingService.start(), threadContext)
        .thenApply(v -> null);
  }

  protected CompletableFuture<Void> joinCluster() {
    persistentMetadataService.addNode(clusterService().getLocalNode());
    return CompletableFuture.completedFuture(null);
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

    closeFuture = leaveCluster()
        .thenComposeAsync(v -> stopServices(), threadContext)
        .thenComposeAsync(v -> completeShutdown(), threadContext);
    return closeFuture;
  }

  protected CompletableFuture<Void> leaveCluster() {
    persistentMetadataService.removeNode(clusterService().getLocalNode());
    return CompletableFuture.completedFuture(null);
  }

  protected CompletableFuture<Void> stopServices() {
    return clusterMessagingService.stop()
        .exceptionally(e -> null)
        .thenComposeAsync(v -> clusterEventingService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> clusterService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> persistentMetadataService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> bootstrapMetadataService.stop(), threadContext)
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
  private static ClusterConfig loadConfig(File config) {
    return Configs.load(config, ClusterConfig.class);
  }

  /**
   * Builds a default messaging service.
   */
  protected static ManagedMessagingService buildMessagingService(ClusterConfig config) {
    return NettyMessagingService.builder()
        .withName(config.getName())
        .withAddress(config.getLocalNode().getAddress())
        .build();
  }

  /**
   * Builds a default broadcast service.
   */
  protected static ManagedBroadcastService buildBroadcastService(ClusterConfig config) {
    return NettyBroadcastService.builder()
        .withLocalAddress(config.getLocalNode().getAddress())
        .withGroupAddress(config.getMulticastAddress())
        .withEnabled(config.isMulticastEnabled())
        .build();
  }

  /**
   * Builds a bootstrap metadata service.
   */
  protected static ManagedBootstrapMetadataService buildBootstrapMetadataService(ClusterConfig config) {
    boolean hasCoreNodes = config.getNodes().stream().anyMatch(node -> node.getType() == Node.Type.PERSISTENT);
    ClusterMetadata metadata = ClusterMetadata.builder()
        .withNodes(config.getNodes()
            .stream()
            .filter(node -> (!hasCoreNodes && node.getType() == Node.Type.EPHEMERAL) || (hasCoreNodes && node.getType() == Node.Type.PERSISTENT))
            .map(Node::new)
            .collect(Collectors.toList()))
        .build();
    return new DefaultBootstrapMetadataService(metadata);
  }

  /**
   * Builds a core metadata service.
   */
  protected static ManagedPersistentMetadataService buildPersistentMetadataService(ClusterConfig config, MessagingService messagingService) {
    ClusterMetadata metadata = ClusterMetadata.builder()
        .withNodes(config.getNodes()
            .stream()
            .filter(node -> node.getType() == Node.Type.PERSISTENT)
            .map(Node::new)
            .collect(Collectors.toList()))
        .build();
    return new DefaultPersistentMetadataService(metadata, messagingService);
  }

  /**
   * Builds a cluster service.
   */
  protected static ManagedClusterService buildClusterService(
      ClusterConfig config,
      BootstrapMetadataService bootstrapMetadataService,
      PersistentMetadataService persistentMetadataService,
      MessagingService messagingService,
      BroadcastService broadcastService) {
    // If the local node has not be configured, create a default node.
    Node localNode;
    if (config.getLocalNode() == null) {
      Address address = Address.empty();
      localNode = Node.builder(address.toString())
          .withType(Node.Type.PERSISTENT)
          .withAddress(address)
          .build();
    } else {
      localNode = new Node(config.getLocalNode());
    }
    return new DefaultClusterService(localNode, bootstrapMetadataService, persistentMetadataService, messagingService, broadcastService);
  }

  /**
   * Builds a cluster messaging service.
   */
  protected static ManagedClusterMessagingService buildClusterMessagingService(
      ClusterService clusterService, MessagingService messagingService) {
    return new DefaultClusterMessagingService(clusterService, messagingService);
  }

  /**
   * Builds a cluster event service.
   */
  protected static ManagedClusterEventingService buildClusterEventService(
      ClusterService clusterService, MessagingService messagingService) {
    return new DefaultClusterEventingService(clusterService, messagingService);
  }

  /**
   * Cluster builder.
   */
  public static class Builder<T extends AtomixCluster<T>> implements io.atomix.utils.Builder<AtomixCluster<T>> {
    protected final ClusterConfig config;

    protected Builder() {
      this(new ClusterConfig());
    }

    protected Builder(ClusterConfig config) {
      this.config = checkNotNull(config);
    }

    /**
     * Sets the cluster name.
     *
     * @param clusterName the cluster name
     * @return the cluster builder
     */
    public Builder withClusterName(String clusterName) {
      config.setName(clusterName);
      return this;
    }

    /**
     * Sets the local node metadata.
     *
     * @param localNode the local node metadata
     * @return the cluster metadata builder
     */
    public Builder withLocalNode(Node localNode) {
      config.setLocalNode(localNode.config());
      return this;
    }

    /**
     * Sets the core nodes.
     *
     * @param coreNodes the core nodes
     * @return the Atomix builder
     */
    public Builder withNodes(Node... coreNodes) {
      return withNodes(Arrays.asList(checkNotNull(coreNodes)));
    }

    /**
     * Sets the core nodes.
     *
     * @param nodes the core nodes
     * @return the Atomix builder
     */
    public Builder withNodes(Collection<Node> nodes) {
      config.setNodes(nodes.stream().map(n -> n.config()).collect(Collectors.toList()));
      return this;
    }

    /**
     * Enables multicast node discovery.
     *
     * @return the Atomix builder
     */
    public Builder withMulticastEnabled() {
      return withMulticastEnabled(true);
    }

    /**
     * Sets whether multicast node discovery is enabled.
     *
     * @param multicastEnabled whether to enable multicast node discovery
     * @return the Atomix builder
     */
    public Builder withMulticastEnabled(boolean multicastEnabled) {
      config.setMulticastEnabled(multicastEnabled);
      return this;
    }

    /**
     * Sets the multicast address.
     *
     * @param address the multicast address
     * @return the Atomix builder
     */
    public Builder withMulticastAddress(Address address) {
      config.setMulticastAddress(address);
      return this;
    }

    @Override
    public AtomixCluster<T> build() {
      return new AtomixCluster<>(config);
    }
  }
}
