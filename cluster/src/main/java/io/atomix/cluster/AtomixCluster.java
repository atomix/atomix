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
import io.atomix.cluster.impl.DefaultClusterMembershipService;
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
  protected final ManagedClusterMembershipService membershipService;
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
    this.messagingService = buildMessagingService(config);
    this.broadcastService = buildBroadcastService(config);
    this.bootstrapMetadataService = buildBootstrapMetadataService(config);
    this.persistentMetadataService = buildPersistentMetadataService(config, messagingService);
    this.membershipService = buildClusterMembershipService(config, bootstrapMetadataService, persistentMetadataService, messagingService, broadcastService);
    this.clusterMessagingService = buildClusterMessagingService(membershipService, messagingService);
    this.clusterEventingService = buildClusterEventService(membershipService, messagingService);
  }

  /**
   * Returns the cluster service.
   *
   * @return the cluster service
   */
  public ClusterMembershipService membershipService() {
    return membershipService;
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
        .thenComposeAsync(v -> membershipService.start(), threadContext)
        .thenComposeAsync(v -> clusterMessagingService.start(), threadContext)
        .thenComposeAsync(v -> clusterEventingService.start(), threadContext)
        .thenApply(v -> null);
  }

  protected CompletableFuture<Void> joinCluster() {
    persistentMetadataService.addMember(membershipService().getLocalMember());
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
    persistentMetadataService.removeMember(membershipService().getLocalMember());
    return CompletableFuture.completedFuture(null);
  }

  protected CompletableFuture<Void> stopServices() {
    return clusterMessagingService.stop()
        .exceptionally(e -> null)
        .thenComposeAsync(v -> clusterEventingService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> membershipService.stop(), threadContext)
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
        .withAddress(config.getLocalMember().getAddress())
        .build();
  }

  /**
   * Builds a default broadcast service.
   */
  protected static ManagedBroadcastService buildBroadcastService(ClusterConfig config) {
    return NettyBroadcastService.builder()
        .withLocalAddress(config.getLocalMember().getAddress())
        .withGroupAddress(config.getMulticastAddress())
        .withEnabled(config.isMulticastEnabled())
        .build();
  }

  /**
   * Builds a bootstrap metadata service.
   */
  protected static ManagedBootstrapMetadataService buildBootstrapMetadataService(ClusterConfig config) {
    boolean hasPersistentNodes = config.getMembers().values().stream().anyMatch(node -> node.getType() == Member.Type.PERSISTENT);
    ClusterMetadata metadata = ClusterMetadata.builder()
        .withNodes(config.getMembers()
            .values()
            .stream()
            .filter(node -> (!hasPersistentNodes && node.getType() == Member.Type.EPHEMERAL) || (hasPersistentNodes && node.getType() == Member.Type.PERSISTENT))
            .map(Member::new)
            .collect(Collectors.toList()))
        .build();
    return new DefaultBootstrapMetadataService(metadata);
  }

  /**
   * Builds a core metadata service.
   */
  protected static ManagedPersistentMetadataService buildPersistentMetadataService(ClusterConfig config, MessagingService messagingService) {
    ClusterMetadata metadata = ClusterMetadata.builder()
        .withNodes(config.getMembers()
            .values()
            .stream()
            .filter(node -> node.getType() == Member.Type.PERSISTENT)
            .map(Member::new)
            .collect(Collectors.toList()))
        .build();
    return new DefaultPersistentMetadataService(metadata, messagingService);
  }

  /**
   * Builds a cluster service.
   */
  protected static ManagedClusterMembershipService buildClusterMembershipService(
      ClusterConfig config,
      BootstrapMetadataService bootstrapMetadataService,
      PersistentMetadataService persistentMetadataService,
      MessagingService messagingService,
      BroadcastService broadcastService) {
    // If the local node has not be configured, create a default node.
    Member localMember;
    if (config.getLocalMember() == null) {
      Address address = Address.empty();
      localMember = Member.builder(address.toString())
          .withType(Member.Type.PERSISTENT)
          .withAddress(address)
          .build();
    } else {
      localMember = new Member(config.getLocalMember());
    }
    return new DefaultClusterMembershipService(localMember, bootstrapMetadataService, persistentMetadataService, messagingService, broadcastService, config.getMembershipConfig());
  }

  /**
   * Builds a cluster messaging service.
   */
  protected static ManagedClusterMessagingService buildClusterMessagingService(
      ClusterMembershipService membershipService, MessagingService messagingService) {
    return new DefaultClusterMessagingService(membershipService, messagingService);
  }

  /**
   * Builds a cluster event service.
   */
  protected static ManagedClusterEventingService buildClusterEventService(
      ClusterMembershipService membershipService, MessagingService messagingService) {
    return new DefaultClusterEventingService(membershipService, messagingService);
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
     * @param localMember the local node metadata
     * @return the cluster metadata builder
     */
    public Builder withLocalMember(Member localMember) {
      config.setLocalMember(localMember.config());
      return this;
    }

    /**
     * Sets the core nodes.
     *
     * @param members the core nodes
     * @return the Atomix builder
     */
    public Builder withMembers(Member... members) {
      return withMembers(Arrays.asList(checkNotNull(members)));
    }

    /**
     * Sets the core nodes.
     *
     * @param members the core nodes
     * @return the Atomix builder
     */
    public Builder withMembers(Collection<Member> members) {
      members.forEach(member -> config.addMember(member.config()));
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
