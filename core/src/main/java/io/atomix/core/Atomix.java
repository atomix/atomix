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
package io.atomix.core;

import com.google.common.collect.Streams;
import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.config.ConfigService;
import io.atomix.primitive.config.impl.DefaultConfigService;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.generator.AtomicIdGenerator;
import io.atomix.core.impl.CorePrimitivesService;
import io.atomix.core.lock.DistributedLock;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.core.multimap.ConsistentMultimap;
import io.atomix.core.profile.Profile;
import io.atomix.core.queue.WorkQueue;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.utils.config.PartitionGroupConfigMapper;
import io.atomix.core.utils.config.PolymorphicConfigMapper;
import io.atomix.core.utils.config.PrimitiveConfigMapper;
import io.atomix.core.utils.config.PrimitiveProtocolConfigMapper;
import io.atomix.core.value.AtomicValue;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.ManagedPartitionService;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.impl.DefaultPartitionService;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.Threads;
import io.atomix.utils.config.ConfigMapper;
import io.atomix.utils.net.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix!
 */
public class Atomix extends AtomixCluster implements PrimitivesService {
  private static final String[] DEFAULT_RESOURCES = new String[]{"atomix", "defaults"};

  private static String[] withDefaultResources(String config) {
    return Streams.concat(Stream.of(config), Stream.of(DEFAULT_RESOURCES)).toArray(String[]::new);
  }

  /**
   * Returns a new Atomix configuration.
   *
   * @return a new Atomix configuration
   */
  public static AtomixConfig config() {
    return config(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix configuration.
   *
   * @param classLoader the class loader
   * @return a new Atomix configuration
   */
  public static AtomixConfig config(ClassLoader classLoader) {
    return config(DEFAULT_RESOURCES, classLoader, AtomixRegistry.registry(classLoader));
  }

  /**
   * Returns a new Atomix configuration from the given resource.
   *
   * @param resource the resource from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given resource
   */
  public static AtomixConfig config(String resource) {
    return config(resource, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix configuration from the given resource.
   *
   * @param resource    the resource from which to return a new Atomix configuration
   * @param classLoader the class loader
   * @return a new Atomix configuration from the given resource
   */
  public static AtomixConfig config(String resource, ClassLoader classLoader) {
    return config(withDefaultResources(resource), classLoader, AtomixRegistry.registry(classLoader));
  }

  /**
   * Returns a new Atomix configuration from the given resources.
   *
   * @param resources   the resources from which to return a new Atomix configuration
   * @param classLoader the class loader
   * @return a new Atomix configuration from the given resource
   */
  private static AtomixConfig config(String[] resources, ClassLoader classLoader, AtomixRegistry registry) {
    ConfigMapper mapper = new PolymorphicConfigMapper(
        classLoader,
        registry,
        new PartitionGroupConfigMapper(),
        new PrimitiveConfigMapper(),
        new PrimitiveProtocolConfigMapper());
    return mapper.loadResources(AtomixConfig.class, resources);
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
    AtomixRegistry registry = AtomixRegistry.registry(classLoader);
    return new Builder(config(DEFAULT_RESOURCES, classLoader, registry), registry);
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
    AtomixRegistry registry = AtomixRegistry.registry(classLoader);
    return new Builder(config(withDefaultResources(config), classLoader, registry), registry);
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config the Atomix configuration
   * @return the Atomix builder
   */
  public static Builder builder(AtomixConfig config) {
    return builder(config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config      the Atomix configuration
   * @param classLoader the class loader with which to load the Atomix registry
   * @return the Atomix builder
   */
  public static Builder builder(AtomixConfig config, ClassLoader classLoader) {
    return new Builder(config, AtomixRegistry.registry(classLoader));
  }

  protected static final Logger LOGGER = LoggerFactory.getLogger(Atomix.class);

  private final ScheduledExecutorService executorService;
  private final AtomixRegistry registry;
  private final ConfigService config;
  private final ManagedPartitionService partitions;
  private final CorePrimitivesService primitives;
  private final boolean enableShutdownHook;
  private final ThreadContext threadContext = new SingleThreadContext("atomix-%d");
  private Thread shutdownHook = null;

  public Atomix(String configFile) {
    this(configFile, Thread.currentThread().getContextClassLoader());
  }

  public Atomix(String configFile, ClassLoader classLoader) {
    this(config(withDefaultResources(configFile), classLoader, AtomixRegistry.registry(classLoader)), AtomixRegistry.registry(classLoader));
  }

  public Atomix(File configFile) {
    this(configFile, Thread.currentThread().getContextClassLoader());
  }

  public Atomix(File configFile, ClassLoader classLoader) {
    this(configFile.getAbsolutePath(), classLoader);
  }

  private Atomix(AtomixConfig config, AtomixRegistry registry) {
    super(config.getClusterConfig());
    config.getProfiles().forEach(profile -> registry.profiles().getProfile(profile).configure(config));
    this.executorService = Executors.newScheduledThreadPool(
        Runtime.getRuntime().availableProcessors(),
        Threads.namedThreads("atomix-primitive-%d", LOGGER));
    this.registry = registry;
    this.config = new DefaultConfigService(config.getPrimitives().values());
    this.partitions = buildPartitionService(config, getMembershipService(), getCommunicationService(), registry);
    this.primitives = new CorePrimitivesService(
        getExecutorService(),
        getMembershipService(),
        getCommunicationService(),
        getEventService(),
        getPartitionService(),
        registry,
        getConfigService());
    this.enableShutdownHook = config.isEnableShutdownHook();
  }

  /**
   * Returns the type registry service.
   *
   * @return the type registry service
   */
  public AtomixRegistry getRegistry() {
    return registry;
  }

  /**
   * Returns the core Atomix executor service.
   *
   * @return the core Atomix executor service
   */
  public ScheduledExecutorService getExecutorService() {
    return executorService;
  }

  /**
   * Returns the primitive configuration service.
   *
   * @return the primitive configuration service
   */
  public ConfigService getConfigService() {
    return config;
  }

  /**
   * Returns the partition service.
   *
   * @return the partition service
   */
  public PartitionService getPartitionService() {
    return partitions;
  }

  /**
   * Returns the primitives service.
   *
   * @return the primitives service
   */
  public PrimitivesService getPrimitivesService() {
    return primitives;
  }

  /**
   * Returns the transaction service.
   *
   * @return the transaction service
   */
  public TransactionService getTransactionService() {
    return primitives.transactionService();
  }

  @Override
  public TransactionBuilder transactionBuilder(String name) {
    return primitives.transactionBuilder(name);
  }

  @Override
  public <B extends DistributedPrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends DistributedPrimitive> B primitiveBuilder(
      String name,
      PrimitiveType<B, C, P> primitiveType) {
    return primitives.primitiveBuilder(name, primitiveType);
  }

  @Override
  public <K, V> ConsistentMap<K, V> getConsistentMap(String name) {
    return primitives.getConsistentMap(name);
  }

  @Override
  public <V> DocumentTree<V> getDocumentTree(String name) {
    return primitives.getDocumentTree(name);
  }

  @Override
  public <V> ConsistentTreeMap<V> getTreeMap(String name) {
    return primitives.getTreeMap(name);
  }

  @Override
  public <K, V> ConsistentMultimap<K, V> getConsistentMultimap(String name) {
    return primitives.getConsistentMultimap(name);
  }

  @Override
  public <K> AtomicCounterMap<K> getAtomicCounterMap(String name) {
    return primitives.getAtomicCounterMap(name);
  }

  @Override
  public <E> DistributedSet<E> getSet(String name) {
    return primitives.getSet(name);
  }

  @Override
  public AtomicCounter getAtomicCounter(String name) {
    return primitives.getAtomicCounter(name);
  }

  @Override
  public AtomicIdGenerator getAtomicIdGenerator(String name) {
    return primitives.getAtomicIdGenerator(name);
  }

  @Override
  public <V> AtomicValue<V> getAtomicValue(String name) {
    return primitives.getAtomicValue(name);
  }

  @Override
  public <T> LeaderElection<T> getLeaderElection(String name) {
    return primitives.getLeaderElection(name);
  }

  @Override
  public <T> LeaderElector<T> getLeaderElector(String name) {
    return primitives.getLeaderElector(name);
  }

  @Override
  public DistributedLock getLock(String name) {
    return primitives.getLock(name);
  }

  @Override
  public DistributedSemaphore getSemaphore(String name) {
    return primitives.getSemaphore(name);
  }

  @Override
  public <E> WorkQueue<E> getWorkQueue(String name) {
    return primitives.getWorkQueue(name);
  }

  @Override
  public <P extends DistributedPrimitive> P getPrimitive(String name, PrimitiveType<?, ?, P> primitiveType) {
    return primitives.getPrimitive(name, primitiveType);
  }

  @Override
  public <C extends PrimitiveConfig<C>, P extends DistributedPrimitive> P getPrimitive(String name, PrimitiveType<?, C, P> primitiveType, C primitiveConfig) {
    return primitives.getPrimitive(name, primitiveType, primitiveConfig);
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives() {
    return primitives.getPrimitives();
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives(PrimitiveType primitiveType) {
    return primitives.getPrimitives(primitiveType);
  }

  @Override
  public <P extends DistributedPrimitive> P getPrimitive(String name) {
    return primitives.getPrimitive(name);
  }

  /**
   * Starts the Atomix instance.
   * <p>
   * The returned future will be completed once this instance completes startup. Note that in order to complete startup,
   * all partitions must be able to form. For Raft partitions, that requires that a majority of the nodes in each
   * partition be started concurrently.
   *
   * @return a future to be completed once the instance has completed startup
   */
  @Override
  public synchronized CompletableFuture<Void> start() {
    if (closeFuture != null) {
      return Futures.exceptionalFuture(new IllegalStateException("Atomix instance " +
          (closeFuture.isDone() ? "shutdown" : "shutting down")));
    }

    return super.start().thenRun(() -> {
      if (enableShutdownHook) {
        if (shutdownHook == null) {
          shutdownHook = new Thread(() -> super.stop().join());
          Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
      }
    });
  }

  @Override
  protected CompletableFuture<Void> startServices() {
    return super.startServices()
        .thenComposeAsync(v -> partitions.start(), threadContext)
        .thenComposeAsync(v -> primitives.start(), threadContext)
        .thenApply(v -> null);
  }

  @Override
  public synchronized CompletableFuture<Void> stop() {
    if (shutdownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        shutdownHook = null;
      } catch (IllegalStateException e) {
        // JVM shutting down
      }
    }
    return super.stop();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected CompletableFuture<Void> stopServices() {
    return primitives.stop()
        .exceptionally(e -> null)
        .thenComposeAsync(v -> partitions.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> super.stopServices(), threadContext);
  }

  @Override
  protected CompletableFuture<Void> completeShutdown() {
    executorService.shutdownNow();
    threadContext.close();
    return super.completeShutdown();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partitions", getPartitionService())
        .toString();
  }

  /**
   * Builds the core partition group.
   */
  @SuppressWarnings("unchecked")
  private static ManagedPartitionGroup buildSystemPartitionGroup(AtomixConfig config) {
    PartitionGroupConfig<?> partitionGroupConfig = config.getManagementGroup();
    if (partitionGroupConfig == null) {
      return null;
    }
    return partitionGroupConfig.getType().newPartitionGroup(partitionGroupConfig);
  }

  /**
   * Builds a partition service.
   */
  @SuppressWarnings("unchecked")
  private static ManagedPartitionService buildPartitionService(
      AtomixConfig config,
      ClusterMembershipService clusterMembershipService,
      ClusterCommunicationService messagingService,
      AtomixRegistry registry) {
    List<ManagedPartitionGroup> partitionGroups = new ArrayList<>();
    for (PartitionGroupConfig<?> partitionGroupConfig : config.getPartitionGroups().values()) {
      partitionGroups.add(partitionGroupConfig.getType().newPartitionGroup(partitionGroupConfig));
    }

    return new DefaultPartitionService(
        clusterMembershipService,
        messagingService,
        registry.primitiveTypes(),
        buildSystemPartitionGroup(config),
        partitionGroups,
        registry.partitionGroupTypes());
  }

  /**
   * Atomix builder.
   */
  public static class Builder extends AtomixCluster.Builder {
    private final AtomixConfig config;
    private final AtomixRegistry registry;

    private Builder(AtomixConfig config, AtomixRegistry registry) {
      super(config.getClusterConfig());
      this.config = checkNotNull(config);
      this.registry = checkNotNull(registry);
    }

    /**
     * Enables the shutdown hook.
     *
     * @return the Atomix builder
     */
    public Builder withShutdownHookEnabled() {
      return withShutdownHook(true);
    }

    /**
     * Enables the shutdown hook.
     *
     * @param enabled if <code>true</code> a shutdown hook will be registered
     * @return the Atomix builder
     */
    public Builder withShutdownHook(boolean enabled) {
      config.setEnableShutdownHook(enabled);
      return this;
    }

    /**
     * Sets the Atomix profiles.
     *
     * @param profiles the profiles
     * @return the Atomix builder
     */
    public Builder withProfiles(Profile... profiles) {
      return withProfiles(Arrays.asList(checkNotNull(profiles)));
    }

    /**
     * Sets the Atomix profiles.
     *
     * @param profiles the profiles
     * @return the Atomix builder
     */
    public Builder withProfiles(Collection<Profile> profiles) {
      profiles.forEach(profile -> config.addProfile(profile.name()));
      return this;
    }

    /**
     * Adds an Atomix profile.
     *
     * @param profile the profile to add
     * @return the Atomix builder
     */
    public Builder addProfile(Profile profile) {
      config.addProfile(profile.name());
      return this;
    }

    /**
     * Sets the system management partition group.
     *
     * @param systemManagementGroup the system management partition group
     * @return the Atomix builder
     */
    public Builder withManagementGroup(ManagedPartitionGroup systemManagementGroup) {
      config.setManagementGroup(systemManagementGroup.config());
      return this;
    }

    /**
     * Sets the partition groups.
     *
     * @param partitionGroups the partition groups
     * @return the Atomix builder
     * @throws NullPointerException if the partition groups are null
     */
    public Builder withPartitionGroups(ManagedPartitionGroup... partitionGroups) {
      return withPartitionGroups(Arrays.asList(checkNotNull(partitionGroups, "partitionGroups cannot be null")));
    }

    /**
     * Sets the partition groups.
     *
     * @param partitionGroups the partition groups
     * @return the Atomix builder
     * @throws NullPointerException if the partition groups are null
     */
    public Builder withPartitionGroups(Collection<ManagedPartitionGroup> partitionGroups) {
      partitionGroups.forEach(group -> config.addPartitionGroup(group.config()));
      return this;
    }

    /**
     * Adds a partition group.
     *
     * @param partitionGroup the partition group to add
     * @return the Atomix builder
     * @throws NullPointerException if the partition group is null
     */
    public Builder addPartitionGroup(ManagedPartitionGroup partitionGroup) {
      config.addPartitionGroup(partitionGroup.config());
      return this;
    }

    @Override
    public Builder withClusterName(String clusterName) {
      super.withClusterName(clusterName);
      return this;
    }

    @Override
    public Builder withLocalMember(Member localMember) {
      super.withLocalMember(localMember);
      return this;
    }

    @Override
    public Builder withMembers(Member... members) {
      super.withMembers(members);
      return this;
    }

    @Override
    public Builder withMembers(Collection<Member> members) {
      super.withMembers(members);
      return this;
    }

    @Override
    public Builder withMulticastEnabled() {
      super.withMulticastEnabled();
      return this;
    }

    @Override
    public Builder withMulticastEnabled(boolean multicastEnabled) {
      super.withMulticastEnabled(multicastEnabled);
      return this;
    }

    @Override
    public Builder withMulticastAddress(Address address) {
      super.withMulticastAddress(address);
      return this;
    }

    /**
     * Builds a new Atomix instance.
     *
     * @return a new Atomix instance
     */
    @Override
    public Atomix build() {
      return new Atomix(config, registry);
    }
  }
}
