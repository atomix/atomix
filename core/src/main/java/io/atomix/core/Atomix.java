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
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.discovery.NodeDiscoveryConfig;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.core.barrier.DistributedCyclicBarrier;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.DistributedCounter;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElector;
import io.atomix.core.idgenerator.AtomicIdGenerator;
import io.atomix.core.impl.CorePrimitiveCache;
import io.atomix.core.impl.CorePrimitivesService;
import io.atomix.core.impl.CoreSerializationService;
import io.atomix.core.list.DistributedList;
import io.atomix.core.lock.AtomicLock;
import io.atomix.core.lock.DistributedLock;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicNavigableMap;
import io.atomix.core.map.AtomicSortedMap;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedNavigableMap;
import io.atomix.core.map.DistributedSortedMap;
import io.atomix.core.multimap.AtomicMultimap;
import io.atomix.core.multimap.DistributedMultimap;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.profile.Profile;
import io.atomix.core.profile.ProfileConfig;
import io.atomix.core.queue.DistributedQueue;
import io.atomix.core.semaphore.AtomicSemaphore;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.set.DistributedNavigableSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSortedSet;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.tree.AtomicDocumentTree;
import io.atomix.core.utils.config.PolymorphicConfigMapper;
import io.atomix.core.utils.config.PolymorphicTypeMapper;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.DistributedValue;
import io.atomix.core.workqueue.WorkQueue;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
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
import io.atomix.primitive.serialization.SerializationService;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.Threads;
import io.atomix.utils.config.ConfigMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

/**
 * Primary interface for managing Atomix clusters and operating on distributed primitives.
 * <p>
 * The {@code Atomix} class is the primary interface to all Atomix features. To construct an {@code Atomix} instance,
 * either configure the instance with a configuration file or construct a new instance from an {@link AtomixBuilder}.
 * Builders can be created via various {@link #builder()} methods:
 * <pre>
 *   {@code
 *   Atomix atomix = Atomix.builder()
 *     .withMemberId("member-1")
 *     .withAddress("localhost:5000")
 *     .build();
 *   }
 * </pre>
 * Once an {@code Atomix} instance has been constructed, start the instance by calling {@link #start()}:
 * <pre>
 *   {@code
 *   atomix.start().join();
 *   }
 * </pre>
 * The returned {@link CompletableFuture} will be completed once the node has been bootstrapped and all services are
 * available.
 * <p>
 * The instance can be used to access services for managing the cluster or communicating with other nodes. Additionally,
 * it provides various methods for creating and operating on distributed primitives. Generally, the primitive methods
 * are separated into two types. Primitive getters return multiton instances of a primitive. Primitives created via
 * getters must be pre-configured in the Atomix instance configuration. Alternatively, primitive builders can be used
 * to create and configure primitives in code:
 * <pre>
 *   {@code
 *   AtomicMap<String, String> map = atomix.mapBuilder("my-map")
 *     .withProtocol(MultiRaftProtocol.builder("raft")
 *       .withReadConsistency(ReadConsistency.SEQUENTIAL)
 *       .build())
 *     .build();
 *   }
 * </pre>
 * Custom primitives can be constructed by providing a custom {@link PrimitiveType} and using the
 * {@link #primitiveBuilder(String, PrimitiveType)} method:
 * <pre>
 *   {@code
 *   MyPrimitive myPrimitive = atomix.primitiveBuilder("my-primitive, MyPrimitiveType.instance())
 *     .withProtocol(MultiRaftProtocol.builder("raft")
 *       .withReadConsistency(ReadConsistency.SEQUENTIAL)
 *       .build())
 *     .build();
 *   }
 * </pre>
 */
public class Atomix extends AtomixCluster implements PrimitivesService {
  private static final List<String> RESOURCES = Arrays.asList("atomix", "defaults");

  /**
   * Returns a new Atomix configuration.
   * <p>
   * The configuration will be loaded from {@code atomix.conf}, {@code atomix.json}, or {@code atomix.properties}
   * if located on the classpath.
   *
   * @return a new Atomix configuration
   */
  public static AtomixConfig config() {
    return config(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix configuration.
   * <p>
   * The configuration will be loaded from {@code atomix.conf}, {@code atomix.json}, or {@code atomix.properties}
   * if located on the classpath.
   *
   * @param classLoader the class loader
   * @return a new Atomix configuration
   */
  public static AtomixConfig config(ClassLoader classLoader) {
    return config(classLoader, null, AtomixRegistry.registry(classLoader));
  }

  /**
   * Returns a new Atomix configuration from the given file.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code atomix.json},
   * or {@code atomix.properties} if located on the classpath.
   *
   * @param files the file from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(String... files) {
    return config(Thread.currentThread().getContextClassLoader(), Stream.of(files).map(File::new).collect(Collectors.toList()));
  }

  /**
   * Returns a new Atomix configuration from the given file.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code atomix.json},
   * or {@code atomix.properties} if located on the classpath.
   *
   * @param classLoader the class loader
   * @param files       the file from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(ClassLoader classLoader, String... files) {
    return config(classLoader, Stream.of(files).map(File::new).collect(Collectors.toList()), AtomixRegistry.registry(classLoader));
  }

  /**
   * Returns a new Atomix configuration.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code atomix.json},
   * or {@code atomix.properties} if located on the classpath.
   *
   * @param configFiles the Atomix configuration files
   * @return a new Atomix configuration
   */
  public static AtomixConfig config(File... configFiles) {
    return config(Thread.currentThread().getContextClassLoader(), Arrays.asList(configFiles), AtomixRegistry.registry());
  }

  /**
   * Returns a new Atomix configuration from the given file.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code atomix.json},
   * or {@code atomix.properties} if located on the classpath.
   *
   * @param files the file from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(List<File> files) {
    return config(Thread.currentThread().getContextClassLoader(), files);
  }

  /**
   * Returns a new Atomix configuration from the given file.
   * <p>
   * The configuration will be loaded from the given file and will fall back to {@code atomix.conf}, {@code atomix.json},
   * or {@code atomix.properties} if located on the classpath.
   *
   * @param classLoader the class loader
   * @param files       the file from which to return a new Atomix configuration
   * @return a new Atomix configuration from the given file
   */
  public static AtomixConfig config(ClassLoader classLoader, List<File> files) {
    return config(classLoader, files, AtomixRegistry.registry(classLoader));
  }

  /**
   * Returns a new Atomix configuration from the given resources.
   *
   * @param classLoader the class loader
   * @param files       the files to load
   * @param registry    the Atomix registry from which to map types
   * @return a new Atomix configuration from the given resource
   */
  private static AtomixConfig config(ClassLoader classLoader, List<File> files, AtomixRegistry registry) {
    ConfigMapper mapper = new PolymorphicConfigMapper(
        classLoader,
        registry,
        new PolymorphicTypeMapper(PartitionGroupConfig.class, PartitionGroup.Type.class),
        new PolymorphicTypeMapper(PrimitiveConfig.class, PrimitiveType.class),
        new PolymorphicTypeMapper(PrimitiveProtocolConfig.class, PrimitiveProtocol.Type.class),
        new PolymorphicTypeMapper(ProfileConfig.class, Profile.Type.class),
        new PolymorphicTypeMapper(NodeDiscoveryConfig.class, NodeDiscoveryProvider.Type.class));
    return mapper.loadFiles(AtomixConfig.class, files, RESOURCES);
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The builder will be initialized with the configuration in {@code atomix.conf}, {@code atomix.json},
   * or {@code atomix.properties} if located on the classpath.
   *
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder() {
    return builder(Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The builder will be initialized with the configuration in {@code atomix.conf}, {@code atomix.json},
   * or {@code atomix.properties} if located on the classpath.
   *
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder(ClassLoader classLoader) {
    AtomixRegistry registry = AtomixRegistry.registry(classLoader);
    return new AtomixBuilder(config(classLoader, null, registry), registry);
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The builder will be initialized with the configuration in the given file and will fall back to {@code atomix.conf},
   * {@code atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param config the Atomix configuration
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder(String config) {
    return builder(config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The builder will be initialized with the configuration in the given file and will fall back to {@code atomix.conf},
   * {@code atomix.json}, or {@code atomix.properties} if located on the classpath.
   *
   * @param configFile  the Atomix configuration file
   * @param classLoader the class loader
   * @return a new Atomix builder
   */
  public static AtomixBuilder builder(String configFile, ClassLoader classLoader) {
    AtomixRegistry registry = AtomixRegistry.registry(classLoader);
    return new AtomixBuilder(config(classLoader, Collections.singletonList(new File(configFile)), registry), registry);
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The returned builder will be initialized with the provided configuration.
   *
   * @param config the Atomix configuration
   * @return the Atomix builder
   */
  public static AtomixBuilder builder(AtomixConfig config) {
    return builder(config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Returns a new Atomix builder.
   * <p>
   * The returned builder will be initialized with the provided configuration.
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
  private final SerializationService serializationService;
  private final ManagedPartitionService partitions;
  private final CorePrimitivesService primitives;
  private final boolean enableShutdownHook;
  private final ThreadContext threadContext = new SingleThreadContext("atomix-%d");
  private Thread shutdownHook = null;

  public Atomix(String... configFiles) {
    this(Thread.currentThread().getContextClassLoader(), configFiles);
  }

  public Atomix(ClassLoader classLoader, String... configFiles) {
    this(classLoader, Stream.of(configFiles).map(File::new).collect(Collectors.toList()));
  }

  public Atomix(File... configFiles) {
    this(Thread.currentThread().getContextClassLoader(), configFiles);
  }

  public Atomix(ClassLoader classLoader, File... configFiles) {
    this(classLoader, Arrays.asList(configFiles));
  }

  public Atomix(ClassLoader classLoader, List<File> configFiles) {
    this(config(classLoader, configFiles, AtomixRegistry.registry(classLoader)), AtomixRegistry.registry(classLoader));
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
    this.serializationService = new CoreSerializationService(config.isTypeRegistrationRequired(), config.isCompatibleSerialization());
    this.partitions = buildPartitionService(config, getMembershipService(), getCommunicationService(), registry);
    this.primitives = new CorePrimitivesService(
        getExecutorService(),
        getMembershipService(),
        getCommunicationService(),
        getEventService(),
        getSerializationService(),
        getPartitionService(),
        new CorePrimitiveCache(),
        registry,
        getConfigService());
    this.enableShutdownHook = config.isEnableShutdownHook();
  }

  /**
   * Returns the Atomix registry service.
   * <p>
   * The registry contains references to all registered Atomix extensions.
   *
   * @return the Atomix registry service
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
   * <p>
   * The primitive configuration service provides all pre-defined named primitive configurations.
   *
   * @return the primitive configuration service
   */
  public ConfigService getConfigService() {
    return config;
  }

  /**
   * Returns the primitive serialization service.
   *
   * @return the primitive serialization service
   */
  public SerializationService getSerializationService() {
    return serializationService;
  }

  /**
   * Returns the partition service.
   * <p>
   * The partition service is responsible for managing the lifecycle of primitive partitions and can provide information
   * about active partition groups and partitions in the cluster.
   *
   * @return the partition service
   */
  public PartitionService getPartitionService() {
    return partitions;
  }

  /**
   * Returns the primitives service.
   * <p>
   * The primitives service is responsible for managing the lifecycle of local primitive instances and can provide
   * information about all primitives registered in the cluster.
   *
   * @return the primitives service
   */
  public PrimitivesService getPrimitivesService() {
    return primitives;
  }

  /**
   * Returns the transaction service.
   * <p>
   * The transaction service is responsible for managing the lifecycle of all transactions in the cluster and can provide
   * information about currently active transactions.
   *
   * @return the transaction service
   */
  public TransactionService getTransactionService() {
    return primitives.transactionService();
  }

  @Override
  public TransactionBuilder transactionBuilder(String name) {
    checkRunning();
    return primitives.transactionBuilder(name);
  }

  @Override
  public <B extends PrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends SyncPrimitive> B primitiveBuilder(
      String name,
      PrimitiveType<B, C, P> primitiveType) {
    checkRunning();
    return primitives.primitiveBuilder(name, primitiveType);
  }

  @Override
  public <K, V> DistributedMap<K, V> getMap(String name) {
    checkRunning();
    return primitives.getMap(name);
  }

  @Override
  public <K extends Comparable<K>, V> DistributedSortedMap<K, V> getSortedMap(String name) {
    checkRunning();
    return primitives.getSortedMap(name);
  }

  @Override
  public <K extends Comparable<K>, V> DistributedNavigableMap<K, V> getNavigableMap(String name) {
    checkRunning();
    return primitives.getNavigableMap(name);
  }

  @Override
  public <K, V> DistributedMultimap<K, V> getMultimap(String name) {
    checkRunning();
    return primitives.getMultimap(name);
  }

  @Override
  public <K, V> AtomicMap<K, V> getAtomicMap(String name) {
    checkRunning();
    return primitives.getAtomicMap(name);
  }

  @Override
  public <V> AtomicDocumentTree<V> getAtomicDocumentTree(String name) {
    checkRunning();
    return primitives.getAtomicDocumentTree(name);
  }

  @Override
  public <K extends Comparable<K>, V> AtomicSortedMap<K, V> getAtomicSortedMap(String name) {
    checkRunning();
    return primitives.getAtomicSortedMap(name);
  }

  @Override
  public <K extends Comparable<K>, V> AtomicNavigableMap<K, V> getAtomicNavigableMap(String name) {
    checkRunning();
    return primitives.getAtomicNavigableMap(name);
  }

  @Override
  public <K, V> AtomicMultimap<K, V> getAtomicMultimap(String name) {
    checkRunning();
    return primitives.getAtomicMultimap(name);
  }

  @Override
  public <K> AtomicCounterMap<K> getAtomicCounterMap(String name) {
    checkRunning();
    return primitives.getAtomicCounterMap(name);
  }

  @Override
  public <E> DistributedSet<E> getSet(String name) {
    checkRunning();
    return primitives.getSet(name);
  }

  @Override
  public <E extends Comparable<E>> DistributedSortedSet<E> getSortedSet(String name) {
    checkRunning();
    return primitives.getSortedSet(name);
  }

  @Override
  public <E extends Comparable<E>> DistributedNavigableSet<E> getNavigableSet(String name) {
    checkRunning();
    return primitives.getNavigableSet(name);
  }

  @Override
  public <E> DistributedQueue<E> getQueue(String name) {
    checkRunning();
    return primitives.getQueue(name);
  }

  @Override
  public <E> DistributedList<E> getList(String name) {
    checkRunning();
    return primitives.getList(name);
  }

  @Override
  public <E> DistributedMultiset<E> getMultiset(String name) {
    checkRunning();
    return primitives.getMultiset(name);
  }

  @Override
  public DistributedCounter getCounter(String name) {
    checkRunning();
    return primitives.getCounter(name);
  }

  @Override
  public AtomicCounter getAtomicCounter(String name) {
    checkRunning();
    return primitives.getAtomicCounter(name);
  }

  @Override
  public AtomicIdGenerator getAtomicIdGenerator(String name) {
    checkRunning();
    return primitives.getAtomicIdGenerator(name);
  }

  @Override
  public <V> DistributedValue<V> getValue(String name) {
    checkRunning();
    return primitives.getValue(name);
  }

  @Override
  public <V> AtomicValue<V> getAtomicValue(String name) {
    checkRunning();
    return primitives.getAtomicValue(name);
  }

  @Override
  public <T> LeaderElection<T> getLeaderElection(String name) {
    checkRunning();
    return primitives.getLeaderElection(name);
  }

  @Override
  public <T> LeaderElector<T> getLeaderElector(String name) {
    checkRunning();
    return primitives.getLeaderElector(name);
  }

  @Override
  public DistributedLock getLock(String name) {
    checkRunning();
    return primitives.getLock(name);
  }

  @Override
  public AtomicLock getAtomicLock(String name) {
    checkRunning();
    return primitives.getAtomicLock(name);
  }

  @Override
  public DistributedCyclicBarrier getCyclicBarrier(String name) {
    checkRunning();
    return primitives.getCyclicBarrier(name);
  }

  @Override
  public DistributedSemaphore getSemaphore(String name) {
    checkRunning();
    return primitives.getSemaphore(name);
  }

  @Override
  public AtomicSemaphore getAtomicSemaphore(String name) {
    checkRunning();
    return primitives.getAtomicSemaphore(name);
  }

  @Override
  public <E> WorkQueue<E> getWorkQueue(String name) {
    checkRunning();
    return primitives.getWorkQueue(name);
  }

  @Override
  public <P extends SyncPrimitive> CompletableFuture<P> getPrimitiveAsync(String name) {
    checkRunning();
    return primitives.getPrimitiveAsync(name);
  }

  @Override
  public <P extends SyncPrimitive> CompletableFuture<P> getPrimitiveAsync(String name, PrimitiveType<?, ?, P> primitiveType) {
    checkRunning();
    return primitives.getPrimitiveAsync(name, primitiveType);
  }

  @Override
  public <C extends PrimitiveConfig<C>, P extends SyncPrimitive> CompletableFuture<P> getPrimitiveAsync(
      String name, PrimitiveType<?, C, P> primitiveType, C primitiveConfig) {
    checkRunning();
    return primitives.getPrimitiveAsync(name, primitiveType, primitiveConfig);
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives() {
    checkRunning();
    return primitives.getPrimitives();
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives(PrimitiveType primitiveType) {
    checkRunning();
    return primitives.getPrimitives(primitiveType);
  }

  /**
   * Checks that the instance is running.
   */
  private void checkRunning() {
    checkState(isRunning(), "Atomix instance is not running");
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
