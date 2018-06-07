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
import io.atomix.cluster.impl.DefaultClusterMembershipService;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster configuration.
 */
public class AtomixCluster implements Managed<Void> {
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
  public static Builder builder() {
    return builder(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static Builder builder(ClassLoader classLoader) {
    return builder(config(DEFAULT_RESOURCES, classLoader));
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config the Atomix configuration
   * @return a new Atomix builder
   */
  public static Builder builder(String config) {
    return builder(config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config      the Atomix configuration
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static Builder builder(String config, ClassLoader classLoader) {
    return new Builder(config(withDefaultResources(config), classLoader));
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config the Atomix configuration
   * @return a new Atomix builder
   */
  public static Builder builder(ClusterConfig config) {
    return new Builder(config);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AtomixCluster.class);

  protected final ManagedMessagingService messagingService;
  protected final ManagedBroadcastService broadcastService;
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
    this.membershipService = buildClusterMembershipService(config, messagingService, broadcastService);
    this.communicationService = buildClusterMessagingService(membershipService, messagingService);
    this.eventService = buildClusterEventService(membershipService, messagingService);
  }

  /**
   * Returns the broadcast service.
   *
   * @return the broadcast service
   */
  public BroadcastService getBroadcastService() {
    return broadcastService;
  }

  /**
   * Returns the messaging service.
   *
   * @return the messaging service
   */
  public MessagingService getMessagingService() {
    return messagingService;
  }

  /**
   * Returns the cluster service.
   *
   * @return the cluster service
   */
  public ClusterMembershipService getMembershipService() {
    return membershipService;
  }

  /**
   * Returns the cluster communication service.
   *
   * @return the cluster communication service
   */
  public ClusterCommunicationService getCommunicationService() {
    return communicationService;
  }

  /**
   * Returns the cluster event service.
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
   * Builds a cluster service.
   */
  protected static ManagedClusterMembershipService buildClusterMembershipService(
      ClusterConfig config,
      MessagingService messagingService,
      BroadcastService broadcastService) {
    // If the local node has not be configured, create a default node.
    Member localMember;
    if (config.getLocalMember() == null) {
      Address address = Address.local();
      localMember = Member.member(address);
    } else {
      localMember = new Member(config.getLocalMember());
    }
    return new DefaultClusterMembershipService(
        localMember,
        config.getMembers()
            .stream()
            .map(Member::new)
            .collect(Collectors.toList()), messagingService, broadcastService, config.getMembershipConfig());
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

  /**
   * Cluster builder.
   */
  public static class Builder implements io.atomix.utils.Builder<AtomixCluster> {
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
     * Sets the local member name.
     *
     * @param localMember the local member name
     * @return the cluster builder
     */
    public Builder withLocalMember(String localMember) {
      config.setLocalMemberId(localMember);
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
    public AtomixCluster build() {
      return new AtomixCluster(config);
    }
  }
}
