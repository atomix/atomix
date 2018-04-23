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

import io.atomix.cluster.AtomixCluster;
import io.atomix.cluster.Node;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.generator.AtomicIdGenerator;
import io.atomix.core.generator.impl.IdGeneratorSessionIdService;
import io.atomix.core.impl.CorePrimitivesService;
import io.atomix.core.lock.DistributedLock;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.core.multimap.ConsistentMultimap;
import io.atomix.core.profile.Profile;
import io.atomix.core.queue.WorkQueue;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.value.AtomicValue;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.ManagedPartitionService;
import io.atomix.primitive.partition.ManagedPrimaryElectionService;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionGroups;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.impl.DefaultPartitionManagementService;
import io.atomix.primitive.partition.impl.DefaultPartitionService;
import io.atomix.primitive.partition.impl.DefaultPrimaryElectionService;
import io.atomix.primitive.partition.impl.HashBasedPrimaryElectionService;
import io.atomix.primitive.session.ManagedSessionIdService;
import io.atomix.primitive.session.impl.DefaultSessionIdService;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.Threads;
import io.atomix.utils.config.Configs;
import io.atomix.utils.config.ConfigurationException;
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
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix!
 */
public class Atomix extends AtomixCluster<Atomix> implements PrimitivesService, Managed<Atomix> {

  /**
   * Returns a new Atomix builder.
   *
   * @return a new Atomix builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config the Atomix configuration
   * @return a new Atomix builder
   */
  public static Builder builder(String config) {
    return new Builder(loadConfig(config));
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param configFile the configuration file with which to initialize the builder
   * @return a new Atomix builder
   */
  public static Builder builder(File configFile) {
    return new Builder(loadConfig(configFile));
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config the Atomix configuration
   * @return a new Atomix builder
   */
  public static Builder builder(AtomixConfig config) {
    return new Builder(config);
  }

  protected static final Logger LOGGER = LoggerFactory.getLogger(Atomix.class);

  private final ScheduledExecutorService executorService;
  private final ManagedPartitionGroup systemPartitionGroup;
  private final ManagedPartitionService partitions;
  private final ManagedPrimitivesService primitives;
  private final PrimitiveTypeRegistry primitiveTypes;
  private final boolean enableShutdownHook;
  private final ThreadContext threadContext = new SingleThreadContext("atomix-%d");
  private Thread shutdownHook = null;

  public Atomix(String configFile) {
    this(loadConfig(new File(System.getProperty("user.dir"), configFile)));
  }

  public Atomix(File configFile) {
    this(loadConfig(configFile));
  }

  public Atomix(AtomixConfig config) {
    super(config.getClusterConfig());
    config.getProfiles().forEach(profile -> profile.configure(config));
    this.executorService = Executors.newScheduledThreadPool(
        Runtime.getRuntime().availableProcessors(),
        Threads.namedThreads("atomix-primitive-%d", LOGGER));
    this.systemPartitionGroup = buildSystemPartitionGroup(config);
    this.partitions = buildPartitionService(config);
    this.primitives = new CorePrimitivesService(
        executorService,
        clusterService(),
        messagingService(),
        eventingService(),
        partitions,
        systemPartitionGroup,
        config);
    this.primitiveTypes = new PrimitiveTypeRegistry(config.getPrimitiveTypes());
    this.enableShutdownHook = config.isEnableShutdownHook();
  }

  /**
   * Returns the partition service.
   *
   * @return the partition service
   */
  public PartitionService partitionService() {
    return partitions;
  }

  /**
   * Returns the primitives service.
   *
   * @return the primitives service
   */
  public PrimitivesService primitivesService() {
    return primitives;
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
  public <E> WorkQueue<E> getWorkQueue(String name) {
    return primitives.getWorkQueue(name);
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
  @SuppressWarnings("unchecked")
  public synchronized CompletableFuture<Atomix> start() {
    if (closeFuture != null) {
      return Futures.exceptionalFuture(new IllegalStateException("Atomix instance " +
          (closeFuture.isDone() ? "shutdown" : "shutting down")));
    }

    return super.start().thenApply(atomix -> {
      if (enableShutdownHook) {
        if (shutdownHook == null) {
          shutdownHook = new Thread(() -> super.stop().join());
          Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
      }
      return atomix;
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  protected CompletableFuture<Void> startServices() {
    return super.startServices()
        .thenComposeAsync(v -> systemPartitionGroup.open(
            new DefaultPartitionManagementService(
                persistentMetadataService,
                clusterService(),
                messagingService(),
                primitiveTypes,
                new HashBasedPrimaryElectionService(clusterService(), messagingService()),
                new DefaultSessionIdService())),
            threadContext)
        .thenComposeAsync(v -> {
          ManagedPrimaryElectionService systemElectionService = new DefaultPrimaryElectionService(systemPartitionGroup);
          ManagedSessionIdService systemSessionIdService = new IdGeneratorSessionIdService(systemPartitionGroup);
          return systemElectionService.start()
              .thenComposeAsync(v2 -> systemSessionIdService.start(), threadContext)
              .thenApply(v2 -> new DefaultPartitionManagementService(
                  persistentMetadataService,
                  clusterService(),
                  messagingService(),
                  primitiveTypes,
                  systemElectionService,
                  systemSessionIdService));
        }, threadContext)
        .thenComposeAsync(partitionManagementService -> partitions.open((PartitionManagementService) partitionManagementService), threadContext)
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
    return partitions.close()
        .exceptionally(e -> null)
        .thenComposeAsync(v -> systemPartitionGroup.close(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> super.stopServices(), threadContext);
  }

  @Override
  protected CompletableFuture<Void> completeShutdown() {
    executorService.shutdownNow();
    return super.completeShutdown();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partitions", partitionService())
        .toString();
  }

  /**
   * Loads a configuration from the given file.
   */
  private static AtomixConfig loadConfig(String config) {
    File configFile = new File(config);
    if (configFile.exists()) {
      return Configs.load(configFile, AtomixConfig.class);
    } else {
      return Configs.load(config, AtomixConfig.class);
    }
  }

  /**
   * Loads a configuration from the given file.
   */
  private static AtomixConfig loadConfig(File config) {
    return Configs.load(config, AtomixConfig.class);
  }

  /**
   * Builds the core partition group.
   */
  private static ManagedPartitionGroup buildSystemPartitionGroup(AtomixConfig config) {
    return config.getSystemPartitionGroup() != null ? PartitionGroups.createGroup(config.getSystemPartitionGroup()) : null;
  }

  /**
   * Builds a partition service.
   */
  private static ManagedPartitionService buildPartitionService(AtomixConfig config) {
    List<ManagedPartitionGroup> partitionGroups = new ArrayList<>();
    for (PartitionGroupConfig partitionGroupConfig : config.getPartitionGroups()) {
      partitionGroups.add(PartitionGroups.createGroup(partitionGroupConfig));
    }

    if (partitionGroups.isEmpty() && config.getSystemPartitionGroup() == null) {
      throw new ConfigurationException("Cannot form data cluster: no system partition group provided");
    }
    return !partitionGroups.isEmpty() ? new DefaultPartitionService(partitionGroups) : null;
  }

  /**
   * Atomix builder.
   */
  public static class Builder extends AtomixCluster.Builder<Atomix> {
    private final AtomixConfig config;

    private Builder() {
      this(new AtomixConfig());
    }

    private Builder(AtomixConfig config) {
      super(config.getClusterConfig());
      this.config = checkNotNull(config);
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
      profiles.forEach(config::addProfile);
      return this;
    }

    /**
     * Adds an Atomix profile.
     *
     * @param profile the profile to add
     * @return the Atomix builder
     */
    public Builder addProfile(Profile profile) {
      config.addProfile(profile);
      return this;
    }

    /**
     * Sets the system partition group.
     *
     * @param systemPartitionGroup the system partition group
     * @return the Atomix builder
     */
    public Builder withSystemPartitionGroup(ManagedPartitionGroup<?> systemPartitionGroup) {
      config.setSystemPartitionGroup(systemPartitionGroup.config());
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
    public Builder withPartitionGroups(Collection<ManagedPartitionGroup<?>> partitionGroups) {
      config.setPartitionGroups(partitionGroups.stream().map(PartitionGroup::config).collect(Collectors.toList()));
      return this;
    }

    /**
     * Adds a partition group.
     *
     * @param partitionGroup the partition group to add
     * @return the Atomix builder
     * @throws NullPointerException if the partition group is null
     */
    public Builder addPartitionGroup(ManagedPartitionGroup<?> partitionGroup) {
      config.addPartitionGroup(partitionGroup.config());
      return this;
    }

    /**
     * Sets the primitive types.
     *
     * @param primitiveTypes the primitive types
     * @return the Atomix builder
     * @throws NullPointerException if the primitive types is {@code null}
     */
    public Builder withPrimitiveTypes(PrimitiveType... primitiveTypes) {
      return withPrimitiveTypes(Arrays.asList(primitiveTypes));
    }

    /**
     * Sets the primitive types.
     *
     * @param primitiveTypes the primitive types
     * @return the Atomix builder
     * @throws NullPointerException if the primitive types is {@code null}
     */
    public Builder withPrimitiveTypes(Collection<PrimitiveType> primitiveTypes) {
      primitiveTypes.forEach(type -> config.addType(type.getClass()));
      return this;
    }

    /**
     * Adds a primitive type.
     *
     * @param primitiveType the primitive type to add
     * @return the Atomix builder
     * @throws NullPointerException if the primitive type is {@code null}
     */
    public Builder addPrimitiveType(PrimitiveType primitiveType) {
      config.addType(primitiveType.getClass());
      return this;
    }

    @Override
    public Builder withClusterName(String clusterName) {
      super.withClusterName(clusterName);
      return this;
    }

    @Override
    public Builder withLocalNode(Node localNode) {
      super.withLocalNode(localNode);
      return this;
    }

    @Override
    public Builder withNodes(Node... coreNodes) {
      super.withNodes(coreNodes);
      return this;
    }

    @Override
    public Builder withNodes(Collection<Node> nodes) {
      super.withNodes(nodes);
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
      return new Atomix(config);
    }
  }
}
