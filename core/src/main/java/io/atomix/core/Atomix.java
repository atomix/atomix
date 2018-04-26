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

import io.atomix.cluster.BootstrapMetadataService;
import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.ManagedBootstrapMetadataService;
import io.atomix.cluster.ManagedClusterService;
import io.atomix.cluster.ManagedPersistentMetadataService;
import io.atomix.cluster.Node;
import io.atomix.cluster.PersistentMetadataService;
import io.atomix.cluster.impl.DefaultBootstrapMetadataService;
import io.atomix.cluster.impl.DefaultClusterService;
import io.atomix.cluster.impl.DefaultPersistentMetadataService;
import io.atomix.cluster.messaging.ClusterEventingService;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.cluster.messaging.ManagedClusterEventingService;
import io.atomix.cluster.messaging.ManagedClusterMessagingService;
import io.atomix.cluster.messaging.impl.DefaultClusterEventingService;
import io.atomix.cluster.messaging.impl.DefaultClusterMessagingService;
import io.atomix.core.config.impl.DefaultConfigService;
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
import io.atomix.core.queue.WorkQueue;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.tree.DocumentTree;
import io.atomix.core.value.AtomicValue;
import io.atomix.messaging.BroadcastService;
import io.atomix.messaging.ManagedBroadcastService;
import io.atomix.messaging.ManagedMessagingService;
import io.atomix.messaging.MessagingService;
import io.atomix.messaging.impl.NettyBroadcastService;
import io.atomix.messaging.impl.NettyMessagingService;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.ManagedPartitionService;
import io.atomix.primitive.partition.ManagedPrimaryElectionService;
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
import io.atomix.utils.ConfigurationException;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.Threads;
import io.atomix.utils.net.Address;
import io.atomix.utils.net.MalformedAddressException;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Atomix!
 */
public class Atomix implements PrimitivesService, Managed<Atomix> {

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

  private final Context context;
  private final AtomicBoolean started = new AtomicBoolean();
  private final ThreadContext threadContext = new SingleThreadContext("atomix-%d");
  private Thread shutdownHook = null;
  private volatile CompletableFuture<Atomix> openFuture;
  private volatile CompletableFuture<Void> closeFuture;

  public Atomix(String configFile) {
    this(loadContext(new File(System.getProperty("user.dir"), configFile)));
  }

  public Atomix(File configFile) {
    this(loadContext(configFile));
  }

  public Atomix(AtomixConfig config) {
    this(buildContext(config));
  }

  private Atomix(Context context) {
    this.context = context;
  }

  /**
   * Returns the cluster service.
   *
   * @return the cluster service
   */
  public ClusterService clusterService() {
    return context.clusterService;
  }

  /**
   * Returns the cluster communication service.
   *
   * @return the cluster communication service
   */
  public ClusterMessagingService messagingService() {
    return context.clusterMessagingService;
  }

  /**
   * Returns the cluster event service.
   *
   * @return the cluster event service
   */
  public ClusterEventingService eventingService() {
    return context.clusterEventingService;
  }

  /**
   * Returns the partition service.
   *
   * @return the partition service
   */
  public PartitionService partitionService() {
    return context.partitions;
  }

  /**
   * Returns the primitives service.
   *
   * @return the primitives service
   */
  public PrimitivesService primitivesService() {
    return context.primitives;
  }

  @Override
  public TransactionBuilder transactionBuilder(String name) {
    return context.primitives.transactionBuilder(name);
  }

  @Override
  public <B extends DistributedPrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends DistributedPrimitive> B primitiveBuilder(
      String name,
      PrimitiveType<B, C, P> primitiveType) {
    return context.primitives.primitiveBuilder(name, primitiveType);
  }

  @Override
  public <K, V> ConsistentMap<K, V> getConsistentMap(String name) {
    return context.primitives.getConsistentMap(name);
  }

  @Override
  public <V> DocumentTree<V> getDocumentTree(String name) {
    return context.primitives.getDocumentTree(name);
  }

  @Override
  public <V> ConsistentTreeMap<V> getTreeMap(String name) {
    return context.primitives.getTreeMap(name);
  }

  @Override
  public <K, V> ConsistentMultimap<K, V> getConsistentMultimap(String name) {
    return context.primitives.getConsistentMultimap(name);
  }

  @Override
  public <K> AtomicCounterMap<K> getAtomicCounterMap(String name) {
    return context.primitives.getAtomicCounterMap(name);
  }

  @Override
  public <E> DistributedSet<E> getSet(String name) {
    return context.primitives.getSet(name);
  }

  @Override
  public AtomicCounter getAtomicCounter(String name) {
    return context.primitives.getAtomicCounter(name);
  }

  @Override
  public AtomicIdGenerator getAtomicIdGenerator(String name) {
    return context.primitives.getAtomicIdGenerator(name);
  }

  @Override
  public <V> AtomicValue<V> getAtomicValue(String name) {
    return context.primitives.getAtomicValue(name);
  }

  @Override
  public <T> LeaderElection<T> getLeaderElection(String name) {
    return context.primitives.getLeaderElection(name);
  }

  @Override
  public <T> LeaderElector<T> getLeaderElector(String name) {
    return context.primitives.getLeaderElector(name);
  }

  @Override
  public DistributedLock getLock(String name) {
    return context.primitives.getLock(name);
  }

  @Override
  public <E> WorkQueue<E> getWorkQueue(String name) {
    return context.primitives.getWorkQueue(name);
  }

  @Override
  public <C extends PrimitiveConfig<C>, P extends DistributedPrimitive> P getPrimitive(String name, PrimitiveType<?, C, P> primitiveType, C primitiveConfig) {
    return context.primitives.getPrimitive(name, primitiveType, primitiveConfig);
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives() {
    return context.primitives.getPrimitives();
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives(PrimitiveType primitiveType) {
    return context.primitives.getPrimitives(primitiveType);
  }

  @Override
  public <P extends DistributedPrimitive> P getPrimitive(String name) {
    return context.primitives.getPrimitive(name);
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
    if (openFuture != null) {
      return openFuture;
    }

    openFuture = context.messagingService.start()
        .thenComposeAsync(v -> context.broadcastService.start(), threadContext)
        .thenComposeAsync(v -> context.bootstrapMetadataService.start(), threadContext)
        .thenComposeAsync(v -> context.coreMetadataService.start(), threadContext)
        .thenComposeAsync(v -> context.clusterService.start(), threadContext)
        .thenComposeAsync(v -> context.clusterMessagingService.start(), threadContext)
        .thenComposeAsync(v -> context.clusterEventingService.start(), threadContext)
        .thenComposeAsync(v -> context.systemPartitionGroup == null
                ? CompletableFuture.completedFuture(null)
                : context.systemPartitionGroup.open(
            new DefaultPartitionManagementService(
                context.coreMetadataService,
                context.clusterService,
                context.clusterMessagingService,
                context.primitiveTypes,
                new HashBasedPrimaryElectionService(context.clusterService, context.clusterMessagingService),
                new DefaultSessionIdService())),
            threadContext)
        .thenComposeAsync(v -> {
          if (context.systemPartitionGroup != null) {
            ManagedPrimaryElectionService systemElectionService = new DefaultPrimaryElectionService(context.systemPartitionGroup);
            ManagedSessionIdService systemSessionIdService = new IdGeneratorSessionIdService(context.systemPartitionGroup);
            return systemElectionService.start()
                .thenComposeAsync(v2 -> systemSessionIdService.start(), threadContext)
                .thenApply(v2 -> new DefaultPartitionManagementService(
                    context.coreMetadataService,
                    context.clusterService,
                    context.clusterMessagingService,
                    context.primitiveTypes,
                    systemElectionService,
                    systemSessionIdService));
          }
          return CompletableFuture.completedFuture(null);
        }, threadContext)
        .thenComposeAsync(partitionManagementService -> partitionManagementService != null
            ? context.partitions.open((PartitionManagementService) partitionManagementService)
            : CompletableFuture.completedFuture(null), threadContext)
        .thenComposeAsync(v -> context.primitives != null
            ? context.primitives.start()
            : CompletableFuture.completedFuture(null), threadContext)
        .thenApplyAsync(v -> {
          context.coreMetadataService.addNode(context.clusterService.getLocalNode());
          started.set(true);
          LOGGER.info("Started");
          return this;
        }, threadContext);

    if (context.enableShutdownHook) {
      if (shutdownHook == null) {
        shutdownHook = new Thread(() -> this.doStop().join());
        Runtime.getRuntime().addShutdownHook(shutdownHook);
      }
    }

    return openFuture;
  }

  @Override
  public boolean isRunning() {
    return started.get();
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
    return doStop();
  }

  private synchronized CompletableFuture<Void> doStop() {
    if (closeFuture != null) {
      return closeFuture;
    }

    context.coreMetadataService.removeNode(context.clusterService.getLocalNode());
    closeFuture = (context.primitives != null ? context.primitives.stop() : CompletableFuture.completedFuture(null))
        .exceptionally(e -> null)
        .thenComposeAsync(v -> context.partitions == null
            ? CompletableFuture.completedFuture(null)
            : context.partitions.close(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> context.systemPartitionGroup == null
            ? CompletableFuture.completedFuture(null)
            : context.systemPartitionGroup.close(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> context.clusterMessagingService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> context.clusterEventingService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> context.clusterService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> context.coreMetadataService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> context.bootstrapMetadataService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> context.broadcastService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenComposeAsync(v -> context.messagingService.stop(), threadContext)
        .exceptionally(e -> null)
        .thenRunAsync(() -> {
          context.executorService.shutdownNow();
          threadContext.close();
          started.set(false);
          LOGGER.info("Stopped");
        });
    return closeFuture;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partitions", partitionService())
        .toString();
  }

  /**
   * Loads a context from the given configuration file.
   */
  private static Context loadContext(File config) {
    return buildContext(loadConfig(config));
  }

  /**
   * Loads a configuration from the given file.
   */
  private static AtomixConfig loadConfig(String config) {
    File configFile = new File(config);
    if (configFile.exists()) {
      return new DefaultConfigService().load(configFile, AtomixConfig.class);
    } else {
      return new DefaultConfigService().load(config, AtomixConfig.class);
    }
  }

  /**
   * Loads a configuration from the given file.
   */
  private static AtomixConfig loadConfig(File config) {
    return new DefaultConfigService().load(config, AtomixConfig.class);
  }

  /**
   * Builds a context from the given configuration.
   */
  private static Context buildContext(AtomixConfig config) {
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), Threads.namedThreads("atomix-primitive-%d", LOGGER));
    ManagedMessagingService messagingService = buildMessagingService(config);
    ManagedBroadcastService broadcastService = buildBroadcastService(config);
    ManagedBootstrapMetadataService bootstrapMetadataService = buildBootstrapMetadataService(config);
    ManagedPersistentMetadataService coreMetadataService = buildCoreMetadataService(config, messagingService);
    ManagedClusterService clusterService = buildClusterService(config, bootstrapMetadataService, coreMetadataService, messagingService, broadcastService);
    ManagedClusterMessagingService clusterMessagingService = buildClusterMessagingService(clusterService, messagingService);
    ManagedClusterEventingService clusterEventingService = buildClusterEventService(clusterService, messagingService);
    ManagedPartitionGroup systemPartitionGroup = buildSystemPartitionGroup(config);
    ManagedPartitionService partitions = buildPartitionService(config);
    ManagedPrimitivesService primitives = partitions != null
        ? new CorePrimitivesService(executorService, clusterService, clusterMessagingService, clusterEventingService, partitions, systemPartitionGroup, config) : null;
    PrimitiveTypeRegistry primitiveTypes = new PrimitiveTypeRegistry(config.getPrimitiveTypes());
    return new Context(
        executorService,
        messagingService,
        broadcastService,
        bootstrapMetadataService,
        coreMetadataService,
        clusterService,
        clusterMessagingService,
        clusterEventingService,
        systemPartitionGroup,
        partitions,
        primitives,
        primitiveTypes,
        config.isEnableShutdownHook());
  }

  /**
   * Builds a default messaging service.
   */
  private static ManagedMessagingService buildMessagingService(AtomixConfig config) {
    return NettyMessagingService.builder()
        .withName(config.getClusterConfig().getName())
        .withAddress(config.getClusterConfig().getLocalNode().getAddress())
        .build();
  }

  /**
   * Builds a default broadcast service.
   */
  private static ManagedBroadcastService buildBroadcastService(AtomixConfig config) {
    return NettyBroadcastService.builder()
        .withLocalAddress(config.getClusterConfig().getLocalNode().getAddress())
        .withGroupAddress(config.getClusterConfig().getMulticastAddress())
        .withEnabled(config.getClusterConfig().isMulticastEnabled())
        .build();
  }

  /**
   * Builds a bootstrap metadata service.
   */
  private static ManagedBootstrapMetadataService buildBootstrapMetadataService(AtomixConfig config) {
    boolean hasCoreNodes = config.getClusterConfig().getNodes().stream().anyMatch(node -> node.getType() == Node.Type.PERSISTENT);
    ClusterMetadata metadata = ClusterMetadata.builder()
        .withNodes(config.getClusterConfig().getNodes()
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
  private static ManagedPersistentMetadataService buildCoreMetadataService(AtomixConfig config, MessagingService messagingService) {
    ClusterMetadata metadata = ClusterMetadata.builder()
        .withNodes(config.getClusterConfig().getNodes()
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
  private static ManagedClusterService buildClusterService(
      AtomixConfig config,
      BootstrapMetadataService bootstrapMetadataService,
      PersistentMetadataService persistentMetadataService,
      MessagingService messagingService,
      BroadcastService broadcastService) {
    // If the local node has not be configured, create a default node.
    Node localNode;
    if (config.getClusterConfig().getLocalNode() == null) {
      Address address = Address.empty();
      localNode = Node.builder(address.toString())
          .withType(Node.Type.PERSISTENT)
          .withAddress(address)
          .build();
    } else {
      localNode = new Node(config.getClusterConfig().getLocalNode());
    }
    return new DefaultClusterService(localNode, bootstrapMetadataService, persistentMetadataService, messagingService, broadcastService);
  }

  /**
   * Builds a cluster messaging service.
   */
  private static ManagedClusterMessagingService buildClusterMessagingService(
      ClusterService clusterService, MessagingService messagingService) {
    return new DefaultClusterMessagingService(clusterService, messagingService);
  }

  /**
   * Builds a cluster event service.
   */
  private static ManagedClusterEventingService buildClusterEventService(
      ClusterService clusterService, MessagingService messagingService) {
    return new DefaultClusterEventingService(clusterService, messagingService);
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
   * Atomix instance context.
   */
  private static class Context {
    private final ScheduledExecutorService executorService;
    private final ManagedMessagingService messagingService;
    private final ManagedBroadcastService broadcastService;
    private final ManagedBootstrapMetadataService bootstrapMetadataService;
    private final ManagedPersistentMetadataService coreMetadataService;
    private final ManagedClusterService clusterService;
    private final ManagedClusterMessagingService clusterMessagingService;
    private final ManagedClusterEventingService clusterEventingService;
    private final ManagedPartitionGroup systemPartitionGroup;
    private final ManagedPartitionService partitions;
    private final ManagedPrimitivesService primitives;
    private final PrimitiveTypeRegistry primitiveTypes;
    private final boolean enableShutdownHook;

    public Context(
        ScheduledExecutorService executorService,
        ManagedMessagingService messagingService,
        ManagedBroadcastService broadcastService,
        ManagedBootstrapMetadataService bootstrapMetadataService,
        ManagedPersistentMetadataService coreMetadataService,
        ManagedClusterService clusterService,
        ManagedClusterMessagingService clusterMessagingService,
        ManagedClusterEventingService clusterEventingService,
        ManagedPartitionGroup systemPartitionGroup,
        ManagedPartitionService partitions,
        ManagedPrimitivesService primitives,
        PrimitiveTypeRegistry primitiveTypes,
        boolean enableShutdownHook) {
      this.executorService = executorService;
      this.messagingService = messagingService;
      this.broadcastService = broadcastService;
      this.bootstrapMetadataService = bootstrapMetadataService;
      this.coreMetadataService = coreMetadataService;
      this.clusterService = clusterService;
      this.clusterMessagingService = clusterMessagingService;
      this.clusterEventingService = clusterEventingService;
      this.systemPartitionGroup = systemPartitionGroup;
      this.partitions = partitions;
      this.primitives = primitives;
      this.primitiveTypes = primitiveTypes;
      this.enableShutdownHook = enableShutdownHook;
    }
  }

  /**
   * Atomix builder.
   */
  public static class Builder implements io.atomix.utils.Builder<Atomix> {
    protected static final String DEFAULT_CLUSTER_NAME = "atomix";

    protected String name = DEFAULT_CLUSTER_NAME;
    protected Node localNode;
    protected Collection<Node> nodes = new ArrayList<>();
    protected boolean multicastEnabled = false;
    protected Address multicastAddress;
    protected ManagedPartitionGroup systemPartitionGroup;
    protected Collection<ManagedPartitionGroup> partitionGroups = new ArrayList<>();
    protected PrimitiveTypeRegistry primitiveTypes = new PrimitiveTypeRegistry();
    protected boolean enableShutdownHook;

    private Builder() {
      try {
        multicastAddress = Address.from("230.0.0.1", 54321);
      } catch (MalformedAddressException e) {
        multicastAddress = Address.from(54321);
      }
    }

    private Builder(AtomixConfig config) {
      this.name = config.getClusterConfig().getName();
      if (config.getClusterConfig().getLocalNode() != null) {
        this.localNode = new Node(config.getClusterConfig().getLocalNode());
      }
      this.nodes = config.getClusterConfig().getNodes().stream().map(Node::new).collect(Collectors.toList());
      this.multicastEnabled = config.getClusterConfig().isMulticastEnabled();
      this.multicastAddress = config.getClusterConfig().getMulticastAddress();
      this.systemPartitionGroup = config.getSystemPartitionGroup() != null ? PartitionGroups.createGroup(config.getSystemPartitionGroup()) : null;
      this.partitionGroups = config.getPartitionGroups().stream().map(PartitionGroups::createGroup).collect(Collectors.toList());
      this.primitiveTypes = new PrimitiveTypeRegistry(config.getPrimitiveTypes());
      this.enableShutdownHook = config.isEnableShutdownHook();
    }

    /**
     * Sets the cluster name.
     *
     * @param name the cluster name
     * @return the cluster metadata builder
     * @throws NullPointerException if the name is null
     */
    public Builder withClusterName(String name) {
      this.name = checkNotNull(name, "name cannot be null");
      return this;
    }

    /**
     * Enables the shutdown hook.
     *
     * @param enable if <code>true</code> a shutdown hook will be registered
     * @return the cluster metadata builder
     */
    public Builder withShutdownHook(boolean enable) {
      this.enableShutdownHook = enable;
      return this;
    }

    /**
     * Sets the local node metadata.
     *
     * @param localNode the local node metadata
     * @return the cluster metadata builder
     */
    public Builder withLocalNode(Node localNode) {
      this.localNode = checkNotNull(localNode, "localNode cannot be null");
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
     * @param coreNodes the core nodes
     * @return the Atomix builder
     */
    public Builder withNodes(Collection<Node> coreNodes) {
      this.nodes = checkNotNull(coreNodes, "coreNodes cannot be null");
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
      this.multicastEnabled = multicastEnabled;
      return this;
    }

    /**
     * Sets the multicast address.
     *
     * @param address the multicast address
     * @return the Atomix builder
     */
    public Builder withMulticastAddress(Address address) {
      this.multicastAddress = checkNotNull(address, "address cannot be null");
      return this;
    }

    /**
     * Sets the system partition group.
     *
     * @param systemPartitionGroup the system partition group
     * @return the Atomix builder
     */
    public Builder withSystemPartitionGroup(ManagedPartitionGroup systemPartitionGroup) {
      this.systemPartitionGroup = systemPartitionGroup;
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
      this.partitionGroups = checkNotNull(partitionGroups, "partitionGroups cannot be null");
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
      partitionGroups.add(partitionGroup);
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
      primitiveTypes.forEach(type -> this.primitiveTypes.register(type));
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
      primitiveTypes.register(primitiveType);
      return this;
    }

    /**
     * Builds a new Atomix instance.
     *
     * @return a new Atomix instance
     */
    @Override
    public Atomix build() {
      ScheduledExecutorService executorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), Threads.namedThreads("atomix-primitive-%d", LOGGER));
      ManagedMessagingService messagingService = buildMessagingService();
      ManagedBroadcastService broadcastService = buildBroadcastService();
      ManagedBootstrapMetadataService bootstrapMetadataService = buildBootstrapMetadataService();
      ManagedPersistentMetadataService coreMetadataService = buildCoreMetadataService(messagingService);
      ManagedClusterService clusterService = buildClusterService(bootstrapMetadataService, coreMetadataService, messagingService, broadcastService);
      ManagedClusterMessagingService clusterMessagingService = buildClusterMessagingService(clusterService, messagingService);
      ManagedClusterEventingService clusterEventingService = buildClusterEventService(clusterService, messagingService);
      ManagedPartitionGroup systemPartitionGroup = buildSystemPartitionGroup();
      ManagedPartitionService partitions = buildPartitionService();
      ManagedPrimitivesService primitives = partitions != null
          ? new CorePrimitivesService(executorService, clusterService, clusterMessagingService, clusterEventingService, partitions, systemPartitionGroup, new AtomixConfig())
          : null;
      return new Atomix(new Context(
          executorService,
          messagingService,
          broadcastService,
          bootstrapMetadataService,
          coreMetadataService,
          clusterService,
          clusterMessagingService,
          clusterEventingService,
          systemPartitionGroup,
          partitions,
          primitives,
          primitiveTypes,
          enableShutdownHook));
    }

    /**
     * Builds a default messaging service.
     */
    protected ManagedMessagingService buildMessagingService() {
      return NettyMessagingService.builder()
          .withName(name)
          .withAddress(localNode.address())
          .build();
    }

    /**
     * Builds a default broadcast service.
     */
    protected ManagedBroadcastService buildBroadcastService() {
      return NettyBroadcastService.builder()
          .withLocalAddress(localNode.address())
          .withGroupAddress(multicastAddress)
          .withEnabled(multicastEnabled)
          .build();
    }

    /**
     * Builds a bootstrap metadata service.
     */
    protected ManagedBootstrapMetadataService buildBootstrapMetadataService() {
      return new DefaultBootstrapMetadataService(ClusterMetadata.builder().withNodes(nodes).build());
    }

    /**
     * Builds a core metadata service.
     */
    protected ManagedPersistentMetadataService buildCoreMetadataService(MessagingService messagingService) {
      return new DefaultPersistentMetadataService(ClusterMetadata.builder()
          .withNodes(nodes.stream()
              .filter(node -> node.type() == Node.Type.PERSISTENT)
              .collect(Collectors.toList()))
          .build(), messagingService);
    }

    /**
     * Builds a cluster service.
     */
    protected ManagedClusterService buildClusterService(
        BootstrapMetadataService bootstrapMetadataService,
        PersistentMetadataService persistentMetadataService,
        MessagingService messagingService,
        BroadcastService broadcastService) {
      return new DefaultClusterService(localNode, bootstrapMetadataService, persistentMetadataService, messagingService, broadcastService);
    }

    /**
     * Builds a cluster messaging service.
     */
    protected ManagedClusterMessagingService buildClusterMessagingService(
        ClusterService clusterService, MessagingService messagingService) {
      return new DefaultClusterMessagingService(clusterService, messagingService);
    }

    /**
     * Builds a cluster event service.
     */
    protected ManagedClusterEventingService buildClusterEventService(
        ClusterService clusterService, MessagingService messagingService) {
      return new DefaultClusterEventingService(clusterService, messagingService);
    }

    /**
     * Builds the core partition group.
     */
    protected ManagedPartitionGroup buildSystemPartitionGroup() {
      return systemPartitionGroup;
    }

    /**
     * Builds a partition service.
     */
    protected ManagedPartitionService buildPartitionService() {
      if (!partitionGroups.isEmpty() && systemPartitionGroup == null) {
        throw new ConfigurationException("Cannot form data cluster: no system partition group provided");
      }
      return !partitionGroups.isEmpty() ? new DefaultPartitionService(partitionGroups) : null;
    }
  }
}
