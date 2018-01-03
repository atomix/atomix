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

import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ClusterMetadataService;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.ManagedClusterMetadataService;
import io.atomix.cluster.ManagedClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.impl.DefaultClusterMetadataService;
import io.atomix.cluster.impl.DefaultClusterService;
import io.atomix.cluster.messaging.ClusterEventingService;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.cluster.messaging.ManagedClusterEventingService;
import io.atomix.cluster.messaging.ManagedClusterMessagingService;
import io.atomix.cluster.messaging.impl.DefaultClusterEventingService;
import io.atomix.cluster.messaging.impl.DefaultClusterMessagingService;
import io.atomix.core.election.impl.LeaderElectorPrimaryElectionService;
import io.atomix.core.generator.impl.IdGeneratorSessionIdService;
import io.atomix.core.impl.CorePrimitivesService;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.ManagedMessagingService;
import io.atomix.messaging.MessagingService;
import io.atomix.messaging.impl.NettyMessagingService;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.ManagedPartitionService;
import io.atomix.primitive.partition.ManagedPrimaryElectionService;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.impl.DefaultPartitionManagementService;
import io.atomix.primitive.partition.impl.DefaultPartitionService;
import io.atomix.primitive.session.ManagedSessionIdService;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
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

  protected static final Logger LOGGER = LoggerFactory.getLogger(Atomix.class);

  private final ManagedMessagingService messagingService;
  private final ManagedClusterMetadataService metadataService;
  private final ManagedClusterService clusterService;
  private final ManagedClusterMessagingService clusterMessagingService;
  private final ManagedClusterEventingService clusterEventingService;
  private final ManagedPartitionGroup corePartitionGroup;
  private final ManagedPartitionService partitions;
  private final ManagedPrimitivesService primitives;
  private final PrimitiveTypeRegistry primitiveTypes;
  private final AtomicBoolean started = new AtomicBoolean();
  private final ThreadContext context = new SingleThreadContext("atomix-%d");
  private volatile CompletableFuture<Atomix> openFuture;
  private volatile CompletableFuture<Void> closeFuture;

  protected Atomix(
      ManagedMessagingService messagingService,
      ManagedClusterMetadataService metadataService,
      ManagedClusterService cluster,
      ManagedClusterMessagingService clusterMessagingService,
      ManagedClusterEventingService clusterEventingService,
      ManagedPartitionGroup corePartitionGroup,
      ManagedPartitionService partitions,
      PrimitiveTypeRegistry primitiveTypes) {
    PrimitiveTypes.register(primitiveTypes);
    this.messagingService = checkNotNull(messagingService, "messagingService cannot be null");
    this.metadataService = checkNotNull(metadataService, "metadataService cannot be null");
    this.clusterService = checkNotNull(cluster, "cluster cannot be null");
    this.clusterMessagingService = checkNotNull(clusterMessagingService, "clusterCommunicator cannot be null");
    this.clusterEventingService = checkNotNull(clusterEventingService, "clusterEventService cannot be null");
    this.corePartitionGroup = checkNotNull(corePartitionGroup, "corePartitionGroup cannot be null");
    this.partitions = checkNotNull(partitions, "partitions cannot be null");
    this.primitiveTypes = checkNotNull(primitiveTypes, "primitiveTypes cannot be null");
    this.primitives = new CorePrimitivesService(cluster, clusterMessagingService, clusterEventingService, partitions);
  }

  /**
   * Returns the cluster metadata service.
   *
   * @return the cluster metadata service
   */
  public ClusterMetadataService metadataService() {
    return metadataService;
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
   * Returns the cluster communication service.
   *
   * @return the cluster communication service
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

  /**
   * Returns a primitive builder of the given type.
   *
   * @param name          the primitive name
   * @param primitiveType the primitive type
   * @param <B>           the primitive builder type
   * @param <P>           the primitive type
   * @return the primitive builder
   */
  public <B extends DistributedPrimitiveBuilder<B, P>, P extends DistributedPrimitive> B primitiveBuilder(
      String name, PrimitiveType<B, P> primitiveType) {
    return primitives.primitiveBuilder(name, primitiveType);
  }

  /**
   * Returns a set of all primitive names of the given type.
   *
   * @param primitiveType the primitive type
   * @return a set of all primitive names of the given type
   */
  @SuppressWarnings("unchecked")
  public Set<String> getPrimitiveNames(PrimitiveType primitiveType) {
    return primitives.getPrimitiveNames(primitiveType);
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
  public synchronized CompletableFuture<Atomix> start() {
    if (openFuture != null) {
      return openFuture;
    }

    openFuture = messagingService.start()
        .thenComposeAsync(v -> metadataService.start(), context)
        .thenComposeAsync(v -> clusterService.start(), context)
        .thenComposeAsync(v -> clusterMessagingService.start(), context)
        .thenComposeAsync(v -> clusterEventingService.start(), context)
        .thenComposeAsync(v -> corePartitionGroup.open(
            new DefaultPartitionManagementService(metadataService, clusterService, clusterMessagingService, primitiveTypes, null, null)), context)
        .thenComposeAsync(v -> {
          ManagedPrimaryElectionService electionService = new LeaderElectorPrimaryElectionService(corePartitionGroup);
          ManagedSessionIdService sessionIdService = new IdGeneratorSessionIdService(corePartitionGroup);
          return electionService.start()
              .thenComposeAsync(v2 -> sessionIdService.start(), context)
              .thenApply(v2 -> new DefaultPartitionManagementService(metadataService, clusterService, clusterMessagingService, primitiveTypes, electionService, sessionIdService));
        }, context)
        .thenComposeAsync(partitionManagementService -> partitions.open(partitionManagementService), context)
        .thenComposeAsync(v -> primitives.start(), context)
        .thenApplyAsync(v -> {
          metadataService.addNode(clusterService.getLocalNode());
          started.set(true);
          LOGGER.info("Started");
          return this;
        }, context);
    return openFuture;
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

    metadataService.removeNode(clusterService.getLocalNode());
    closeFuture = primitives.stop()
        .thenComposeAsync(v -> partitions.close(), context)
        .thenComposeAsync(v -> corePartitionGroup.close(), context)
        .thenComposeAsync(v -> clusterMessagingService.stop(), context)
        .thenComposeAsync(v -> clusterEventingService.stop(), context)
        .thenComposeAsync(v -> clusterService.stop(), context)
        .thenComposeAsync(v -> metadataService.stop(), context)
        .thenComposeAsync(v -> messagingService.stop(), context)
        .thenRunAsync(() -> {
          context.close();
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
   * Atomix builder.
   */
  public static class Builder implements io.atomix.utils.Builder<Atomix> {
    protected static final String DEFAULT_CLUSTER_NAME = "atomix";
    // Default to 7 Raft partitions to allow a leader per node in 7 node clusters
    protected static final int DEFAULT_COORDINATION_PARTITIONS = 7;
    // Default to 3-node partitions for the best latency/throughput per Raft partition
    protected static final int DEFAULT_COORDINATION_PARTITION_SIZE = 3;
    // Default to 71 primary-backup partitions - a prime number that creates about 10 partitions per node in a 7-node cluster
    protected static final int DEFAULT_DATA_PARTITIONS = 71;
    protected static final String COORDINATION_GROUP_NAME = "coordination";
    protected static final String DATA_GROUP_NAME = "data";

    protected String name = DEFAULT_CLUSTER_NAME;
    protected Node localNode;
    protected Collection<Node> bootstrapNodes;
    protected File dataDirectory = new File(System.getProperty("user.dir"), "data");
    protected int numCoordinationPartitions = DEFAULT_COORDINATION_PARTITIONS;
    protected int coordinationPartitionSize = DEFAULT_COORDINATION_PARTITION_SIZE;
    protected int numDataPartitions = DEFAULT_DATA_PARTITIONS;
    protected Collection<ManagedPartitionGroup> partitionGroups = new ArrayList<>();
    protected PrimitiveTypeRegistry primitiveTypes = new PrimitiveTypeRegistry();

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
     * Sets the bootstrap nodes.
     *
     * @param bootstrapNodes the nodes from which to bootstrap the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the bootstrap nodes are {@code null}
     */
    public Builder withBootstrapNodes(Node... bootstrapNodes) {
      return withBootstrapNodes(Arrays.asList(checkNotNull(bootstrapNodes)));
    }

    /**
     * Sets the bootstrap nodes.
     *
     * @param bootstrapNodes the nodes from which to bootstrap the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the bootstrap nodes are {@code null}
     */
    public Builder withBootstrapNodes(Collection<Node> bootstrapNodes) {
      this.bootstrapNodes = checkNotNull(bootstrapNodes, "bootstrapNodes cannot be null");
      return this;
    }

    /**
     * Sets the partition data directory.
     *
     * @param dataDirectory the partition data directory
     * @return the Atomix builder
     * @throws NullPointerException if the data directory is null
     */
    public Builder withDataDirectory(File dataDirectory) {
      this.dataDirectory = checkNotNull(dataDirectory, "dataDirectory cannot be null");
      return this;
    }

    /**
     * Sets the number of coordination (Raft) partitions.
     *
     * @param corePartitions the number of coordination partitions
     * @return the Atomix builder
     * @throws IllegalArgumentException if the number of partitions is not positive
     */
    public Builder withCoordinationPartitions(int corePartitions) {
      checkArgument(corePartitions > 0, "corePartitions must be positive");
      this.numCoordinationPartitions = corePartitions;
      return this;
    }

    /**
     * Sets the coordination (Raft) partition size.
     *
     * @param partitionSize the coordination partition size
     * @return the Atomix builder
     * @throws IllegalArgumentException if the partition size is not positive
     */
    public Builder withCoordinationPartitionSize(int partitionSize) {
      checkArgument(partitionSize > 0, "partitionSize must be positive");
      this.coordinationPartitionSize = partitionSize;
      return this;
    }

    /**
     * Sets the number of data partitions.
     *
     * @param dataPartitions the number of data partitions
     * @return the Atomix builder
     * @throws IllegalArgumentException if the number of data partitions is not positive
     */
    public Builder withDataPartitions(int dataPartitions) {
      checkArgument(dataPartitions > 0, "dataPartitions must be positive");
      this.numDataPartitions = dataPartitions;
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
      // If the local node has not be configured, create a default node.
      if (localNode == null) {
        try {
          InetAddress address = InetAddress.getByName("0.0.0.0");
          localNode = Node.builder(address.getHostName())
              .withType(Node.Type.DATA)
              .withEndpoint(new Endpoint(address, NettyMessagingService.DEFAULT_PORT))
              .build();
        } catch (UnknownHostException e) {
          throw new ConfigurationException("Cannot configure local node", e);
        }
      }

      // If the bootstrap nodes have not been configured, default to the local node if possible.
      if (bootstrapNodes == null) {
        if (localNode.type() == Node.Type.DATA) {
          bootstrapNodes = Collections.singleton(localNode);
        } else {
          throw new ConfigurationException("No bootstrap nodes configured");
        }
      }

      ManagedMessagingService messagingService = buildMessagingService();
      ManagedClusterMetadataService metadataService = buildClusterMetadataService(messagingService);
      ManagedClusterService clusterService = buildClusterService(metadataService, messagingService);
      ManagedClusterMessagingService clusterMessagingService = buildClusterMessagingService(clusterService, messagingService);
      ManagedClusterEventingService clusterEventService = buildClusterEventService(clusterService, messagingService);
      ManagedPartitionGroup corePartitionGroup = buildCorePartitionGroup();
      ManagedPartitionService partitionService = buildPartitionService();
      return new Atomix(
          messagingService,
          metadataService,
          clusterService,
          clusterMessagingService,
          clusterEventService,
          corePartitionGroup,
          partitionService,
          primitiveTypes);
    }

    /**
     * Builds a default messaging service.
     */
    protected ManagedMessagingService buildMessagingService() {
      return NettyMessagingService.builder()
          .withName(name)
          .withEndpoint(localNode.endpoint())
          .build();
    }

    /**
     * Builds a cluster metadata service.
     */
    protected ManagedClusterMetadataService buildClusterMetadataService(MessagingService messagingService) {
      return new DefaultClusterMetadataService(ClusterMetadata.builder().withBootstrapNodes(bootstrapNodes).build(), messagingService);
    }

    /**
     * Builds a cluster service.
     */
    protected ManagedClusterService buildClusterService(ClusterMetadataService metadataService, MessagingService messagingService) {
      return new DefaultClusterService(localNode, metadataService, messagingService);
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
    protected ManagedPartitionGroup buildCorePartitionGroup() {
      return RaftPartitionGroup.builder("core")
          .withNumPartitions(1)
          .withDataDirectory(new File(dataDirectory, "core"))
          .build();
    }

    /**
     * Builds a partition service.
     */
    protected ManagedPartitionService buildPartitionService() {
      if (partitionGroups.isEmpty()) {
        partitionGroups.add(RaftPartitionGroup.builder(COORDINATION_GROUP_NAME)
            .withDataDirectory(new File(dataDirectory, COORDINATION_GROUP_NAME))
            .withNumPartitions(numCoordinationPartitions)
            .withPartitionSize(coordinationPartitionSize)
            .build());
        partitionGroups.add(PrimaryBackupPartitionGroup.builder(DATA_GROUP_NAME)
            .withNumPartitions(numDataPartitions)
            .build());
      }
      return new DefaultPartitionService(partitionGroups);
    }
  }
}
