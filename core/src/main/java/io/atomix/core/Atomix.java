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
import io.atomix.cluster.discovery.NodeDiscoveryConfig;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.core.barrier.DistributedCyclicBarrier;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.countermap.AtomicCounterMap;
import io.atomix.core.idgenerator.AtomicIdGenerator;
import io.atomix.core.impl.CorePrimitivesService;
import io.atomix.core.leadership.LeaderElection;
import io.atomix.core.leadership.LeaderElector;
import io.atomix.core.list.DistributedList;
import io.atomix.core.lock.DistributedLock;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedTreeMap;
import io.atomix.core.multimap.AtomicMultimap;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.profile.Profile;
import io.atomix.core.profile.ProfileConfig;
import io.atomix.core.queue.DistributedQueue;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.tree.AtomicDocumentTree;
import io.atomix.core.map.AtomicTreeMap;
import io.atomix.core.utils.config.PolymorphicConfigMapper;
import io.atomix.core.utils.config.PolymorphicTypeMapper;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.workqueue.WorkQueue;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.config.ConfigService;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.config.impl.DefaultConfigService;
import io.atomix.primitive.impl.DefaultPrimitiveTypeRegistry;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.ManagedPartitionService;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.impl.DefaultPartitionGroupTypeRegistry;
import io.atomix.primitive.partition.impl.DefaultPartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.Threads;
import io.atomix.utils.config.ConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Atomix!
 */
public class Atomix extends AtomixCluster implements PrimitivesService {
  private static final String[] RESOURCES = new String[]{"atomix", "defaults"};

  private static String[] withDefaultResources(String config) {
    return Streams.concat(Stream.of(config), Stream.of(RESOURCES)).toArray(String[]::new);
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
   * @param configFile the Atomix configuration file
   * @return a new Atomix configuration
   */
  public static AtomixConfig config(File configFile) {
    return config(configFile, Thread.currentThread().getContextClassLoader(), AtomixRegistry.registry());
  }

  /**
   * Returns a new Atomix configuration.
   *
   * @param classLoader the class loader
   * @return a new Atomix configuration
   */
  public static AtomixConfig config(ClassLoader classLoader) {
    return config(null, classLoader, AtomixRegistry.registry(classLoader));
  }

  /**
   * Returns a new Atomix configuration from the given file.
   *
   * @param file the file from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(String file) {
    return config(file, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix configuration from the given file.
   *
   * @param file        the file from which to return a new Atomix configuration
   * @param classLoader the class loader
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(String file, ClassLoader classLoader) {
    return config(new File(file), classLoader, AtomixRegistry.registry(classLoader));
  }

  /**
   * Returns a new Atomix configuration from the given resources.
   *
   * @param file        the file to load
   * @param classLoader the class loader
   * @param registry    the Atomix registry from which to map types
   * @return a new Atomix configuration from the given resource
   */
  private static AtomixConfig config(File file, ClassLoader classLoader, AtomixRegistry registry) {
    ConfigMapper mapper = new PolymorphicConfigMapper(
        classLoader,
        registry,
        new PolymorphicTypeMapper(PartitionGroupConfig.class, PartitionGroup.Type.class),
        new PolymorphicTypeMapper(PrimitiveConfig.class, PrimitiveType.class),
        new PolymorphicTypeMapper(PrimitiveProtocolConfig.class, PrimitiveProtocol.Type.class),
        new PolymorphicTypeMapper(ProfileConfig.class, Profile.Type.class),
        new PolymorphicTypeMapper(NodeDiscoveryConfig.class, NodeDiscoveryProvider.Type.class));
    return mapper.loadFile(AtomixConfig.class, file, RESOURCES);
  }

  /**
   * Returns a new Atomix builder.
   *
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder() {
    return builder(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder(ClassLoader classLoader) {
    AtomixRegistry registry = AtomixRegistry.registry(classLoader);
    return new AtomixBuilder(config(null, classLoader, registry), registry);
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config the Atomix configuration
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder(String config) {
    return builder(config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param configFile  the Atomix configuration file
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder(String configFile, ClassLoader classLoader) {
    AtomixRegistry registry = AtomixRegistry.registry(classLoader);
    return new AtomixBuilder(config(new File(configFile), classLoader, registry), registry);
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config the Atomix configuration
   * @return the Atomix builder
   */
  public static AtomixBuilder builder(AtomixConfig config) {
    return builder(config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   *
   * @param config      the Atomix configuration
   * @param classLoader the class loader with which to load the Atomix registry
   * @return the Atomix builder
   */
  public static AtomixBuilder builder(AtomixConfig config, ClassLoader classLoader) {
    return new AtomixBuilder(config, AtomixRegistry.registry(classLoader));
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
    this(new File(configFile), classLoader);
  }

  public Atomix(File configFile) {
    this(configFile, Thread.currentThread().getContextClassLoader());
  }

  public Atomix(File configFile, ClassLoader classLoader) {
    this(config(configFile, classLoader, AtomixRegistry.registry(classLoader)), AtomixRegistry.registry(classLoader));
  }

  @SuppressWarnings("unchecked")
  protected Atomix(AtomixConfig config, AtomixRegistry registry) {
    super(config.getClusterConfig());
    config.getProfiles().forEach(profile -> profile.getType().newProfile(profile).configure(config));
    this.executorService = Executors.newScheduledThreadPool(
        Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 8), 4),
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
  public <B extends PrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends DistributedPrimitive> B primitiveBuilder(
      String name,
      PrimitiveType<B, C, P> primitiveType) {
    return primitives.primitiveBuilder(name, primitiveType);
  }

  @Override
  public <K, V> DistributedMap<K, V> getMap(String name) {
    return primitives.getMap(name);
  }

  @Override
  public <K extends Comparable<K>, V> DistributedTreeMap<K, V> getTreeMap(String name) {
    return primitives.getTreeMap(name);
  }

  @Override
  public <K, V> AtomicMap<K, V> getAtomicMap(String name) {
    return primitives.getAtomicMap(name);
  }

  @Override
  public <V> AtomicDocumentTree<V> getAtomicDocumentTree(String name) {
    return primitives.getAtomicDocumentTree(name);
  }

  @Override
  public <K extends Comparable<K>, V> AtomicTreeMap<K, V> getAtomicTreeMap(String name) {
    return primitives.getAtomicTreeMap(name);
  }

  @Override
  public <K, V> AtomicMultimap<K, V> getAtomicMultimap(String name) {
    return primitives.getAtomicMultimap(name);
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
  public <E> DistributedQueue<E> getQueue(String name) {
    return primitives.getQueue(name);
  }

  @Override
  public <E> DistributedList<E> getList(String name) {
    return primitives.getList(name);
  }

  @Override
  public <E> DistributedMultiset<E> getMultiset(String name) {
    return primitives.getMultiset(name);
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
  public DistributedCyclicBarrier getCyclicBarrier(String name) {
    return primitives.getCyclicBarrier(name);
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
        new DefaultPrimitiveTypeRegistry(registry.getTypes(PrimitiveType.class)),
        buildSystemPartitionGroup(config),
        partitionGroups,
        new DefaultPartitionGroupTypeRegistry(registry.getTypes(PartitionGroup.Type.class)));
  }
}
