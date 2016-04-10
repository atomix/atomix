/*
 * Copyright 2015 the original author or authors.
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
package io.atomix;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.*;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.cluster.ClusterManager;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.Cluster;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.session.Session;
import io.atomix.manager.ResourceClient;
import io.atomix.manager.ResourceServer;
import io.atomix.manager.internal.ResourceManagerException;
import io.atomix.manager.internal.ResourceManagerState;
import io.atomix.manager.options.ServerOptions;
import io.atomix.manager.util.ResourceManagerTypeResolver;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceType;
import io.atomix.resource.internal.ResourceRegistry;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Provides an interface for creating and operating on {@link io.atomix.resource.Resource}s as a stateful node.
 * <p>
 * Replicas serve as a hybrid {@link AtomixClient} and server to allow a server to be embedded
 * in an application. From the perspective of state, replicas behave like servers in that they
 * maintain a replicated state machine for {@link io.atomix.resource.Resource}s and fully participate in the underlying
 * consensus algorithm. From the perspective of resources, replicas behave like {@link AtomixClient}s in that
 * they may themselves create and modify distributed resources.
 * <p>
 * To create a replica, use the {@link #builder(Address)} builder factory. Each replica must
 * be initially configured with a replica {@link Address} and a list of addresses for other members of the
 * core cluster. Note that the list of member addresses does not have to include the local replica nor does
 * it have to include all the replicas in the cluster. As long as the replica can reach one live member of
 * the cluster, it can join.
 * <pre>
 *   {@code
 *   List<Address> members = Arrays.asList(new Address("123.456.789.0", 5000), new Address("123.456.789.1", 5000));
 *   Atomix atomix = AtomixReplica.builder(address, members)
 *     .withTransport(new NettyTransport())
 *     .withStorage(new Storage(StorageLevel.MEMORY))
 *     .build();
 *   }
 * </pre>
 * Replicas must be configured with a {@link Transport} and {@link Storage}. By default, if no transport is
 * configured, the {@code NettyTransport} will be used and will thus be expected to be available on the classpath.
 * Similarly, if no storage module is configured, replicated commit logs will be written to
 * {@code System.getProperty("user.dir")} with a default log name.
 * <h2>Storage</h2>
 * Replicas manage resource and replicate and store resource state changes on disk. In order to do so, users
 * must configure the replica's {@link Storage} configuration via the {@link Builder#withStorage(Storage)} method.
 * Storage does not have to be identical on all replicas, but it is important to the desired level of fault-tolerance,
 * consistency, and performance. Users can configure where and how state changes are stored by configuring the
 * {@link io.atomix.copycat.server.storage.StorageLevel}.
 * <pre>
 *   {@code
 *   Atomix atomix = AtomixReplica.builder(address, members)
 *     .withTransport(new NettyTransport())
 *     .withStorage(Storage.builder()
 *       .withDirectory(new File("logs"))
 *       .withStorageLevel(StorageLevel.MAPPED)
 *       .withMaxSegmentSize(1024 * 1024)
 *       .build())
 *     .build();
 *   }
 * </pre>
 * For the strongest level of consistency, it's recommended that users use the
 * {@link io.atomix.copycat.server.storage.StorageLevel#DISK DISK} storage level. For the greatest mix of consistency
 * and performance, the {@link io.atomix.copycat.server.storage.StorageLevel#MAPPED MAPPED} storage level uses memory
 * mapped files to persist state changes. The {@link io.atomix.copycat.server.storage.StorageLevel#MEMORY MEMORY}
 * storage level is recommended only for testing. Atomix cannot guarantee writes will not be lost with {@code MEMORY}
 * based logs. If a majority of the active replicas in the cluster are lost or partitioned, writes can be overwritten.
 * Using memory-based storage amounts to recreating the entire replica each time it's started.
 * <h2>Replica lifecycle</h2>
 * When the replica is {@link #bootstrap() started}, the replica will attempt to contact members in the configured
 * startup {@link Address} list. If any of the members are already in an active state, the replica will request
 * to join the cluster. During the process of joining the cluster, the replica will notify the current cluster
 * leader of its existence. If the leader already knows about the joining replica, the replica will immediately
 * join and become a full voting member. If the joining replica is not yet known to the rest of the cluster,
 * it will join the cluster in a <em>passive</em> state in which it receives replicated state from other
 * replicas in the cluster but does not participate in elections or other quorum-based aspects of the
 * underlying consensus algorithm. Once the joining replica is caught up with the rest of the cluster, the
 * leader will promote it to a full voting member.
 * <p>
 * Once the replica has joined the cluster, it will persist the updated cluster configuration to disk via
 * the replica's configured {@link Storage} module. This is important to note as in the event that the replica
 * crashes, <em>the replica will recover from its last known configuration</em> rather than the configuration
 * provided to the {@link #builder(Address) builder factory}. This allows Atomix cluster structures
 * to change transparently and independently of the code that configures any given replica. If a persistent
 * {@link io.atomix.copycat.server.storage.StorageLevel} is used, user code should simply configure the replica
 * consistently based on the <em>initial</em> replica configuration, and the replica will recover from the last
 * known cluster configuration in the event of a failure.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class AtomixReplica extends Atomix {

  /**
   * Defines the behavior of a replica within the Atomix cluster.
   * <p>
   * Each replica can be configured with a replica type. The replica type defines how the replica behaves within
   * the Atomix cluster. Throughout the lifetime of a cluster, each replica within the cluster may be promoted
   * or demoted between types by the configured {@link ClusterManager}.
   */
  public enum Type {

    /**
     * Active replicas are full voting members of the Raft cluster.
     * <p>
     * Active replicas are stateful members of the cluster that participate fully in the Raft consensus
     * algorithm. Each Atomix cluster must consist of at least one active replica, and all writes to the
     * cluster go through an active member. The availability of the cluster is dependent on the number of
     * active replicas at any given time. Atomix cluster can tolerate the failure of a minority of active
     * replicas.
     */
    ACTIVE,

    /**
     * Passive replicas are stateful members of the Atomix cluster that participate in replication via
     * an asynchronous gossip protocol.
     * <p>
     * Passive replicas serve as a mechanism to scale writes and to quickly replace failed or partitioned
     * active members. Writes to the cluster are synchronously replicated to active replicas, and once committed
     * are asynchronously replicated to passive replicas. This allows passive replicas to represent state slightly
     * behind the Raft cluster. Reads from passive replicas are still guaranteed to be sequentially consistent.
     * Resources created on a passive replica that execute sequential queries against the cluster will read from
     * the local replica's state, thus increasing read latency significantly. In the event an active replica
     * is partitioned or crashes, the configured {@link ClusterManager} can replace active replicas with passive
     * replicas to reduce the amount of time required to catch a server up with the Raft cluster.
     */
    PASSIVE,

    /**
     * Reserve replicas are stateless members of the Atomix cluster.
     * <p>
     * Reserve replicas act as standby nodes for the rest of the Atomix cluster. {@link ClusterManager}s configured
     * for the cluster may use reserve replicas to replace failed passive or active replicas as necessary.
     */
    RESERVE,
  }

  /**
   * Returns a new Atomix replica builder.
   * <p>
   * The replica {@link Address} is the address to which the replica will bind to communicate with both
   * clients and other replicas and through which clients and replicas will connect to the constructed replica.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster. The members
   * do not have to be representative of the full cluster membership.
   * <p>
   * When starting a new cluster, the cluster should be formed by providing the {@code members} list of the
   * replicas that make up the initial members of the cluster. If the replica being built is included in the
   * initial membership list, its {@link Address} should be listed in the {@code members} list. Otherwise,
   * if the replica is joining an existing cluster, its {@link Address} should not be listed in the membership
   * list.
   * <p>
   * If the replica uses a persistent {@link io.atomix.copycat.server.storage.StorageLevel} like
   * {@link io.atomix.copycat.server.storage.StorageLevel#DISK DISK} or {@link io.atomix.copycat.server.storage.StorageLevel#MAPPED MAPPED}
   * then the provided membership list only applies to the first time the replica is started. Once the replica
   * has been started and joined with other members of the cluster, the updated cluster configuration will be
   * stored on disk, and if the replica crashes and is restarted it will use the persisted configuration rather
   * than the user-provided configuration. This behavior cannot be overridden.
   *
   * @param address The address through which clients and replicas connect to the replica.
   * @return The replica builder.
   */
  public static Builder builder(Address address) {
    return builder(address, address);
  }

  /**
   * Returns a new Atomix replica builder.
   * <p>
   * The provided server {@link Address} is the address to which the replica will bind for communication with
   * other replicas in the cluster.
   * <p>
   * The client {@link Address} is the address to which clients will connect to the replica to open and close
   * resources and submit state change operations.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster. The members
   * do not have to be representative of the full cluster membership.
   * <p>
   * When starting a new cluster, the cluster should be formed by providing the {@code members} list of the
   * replicas that make up the initial members of the cluster. If the replica being built is included in the
   * initial membership list, its {@link Address} should be listed in the {@code members} list. Otherwise,
   * if the replica is joining an existing cluster, its {@link Address} should not be listed in the membership
   * list.
   * <p>
   * If the replica uses a persistent {@link io.atomix.copycat.server.storage.StorageLevel} like
   * {@link io.atomix.copycat.server.storage.StorageLevel#DISK DISK} or {@link io.atomix.copycat.server.storage.StorageLevel#MAPPED MAPPED}
   * then the provided membership list only applies to the first time the replica is started. Once the replica
   * has been started and joined with other members of the cluster, the updated cluster configuration will be
   * stored on disk, and if the replica crashes and is restarted it will use the persisted configuration rather
   * than the user-provided configuration. This behavior cannot be overridden.
   *
   * @param clientAddress The address through which clients connect to the replica.
   * @param serverAddress The address through which other replicas connect to the replica.
   * @return The replica builder.
   */
  public static Builder builder(Address clientAddress, Address serverAddress) {
    return new Builder(clientAddress, serverAddress);
  }

  /**
   * Returns a new Atomix replica builder.
   * <p>
   * The replica {@link Address} is the address to which the replica will bind to communicate with both
   * clients and other replicas and through which clients and replicas will connect to the constructed replica.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster. The members
   * do not have to be representative of the full cluster membership.
   * <p>
   * When starting a new cluster, the cluster should be formed by providing the {@code members} list of the
   * replicas that make up the initial members of the cluster. If the replica being built is included in the
   * initial membership list, its {@link Address} should be listed in the {@code members} list. Otherwise,
   * if the replica is joining an existing cluster, its {@link Address} should not be listed in the membership
   * list.
   * <p>
   * If the replica uses a persistent {@link io.atomix.copycat.server.storage.StorageLevel} like
   * {@link io.atomix.copycat.server.storage.StorageLevel#DISK DISK} or {@link io.atomix.copycat.server.storage.StorageLevel#MAPPED MAPPED}
   * then the provided membership list only applies to the first time the replica is started. Once the replica
   * has been started and joined with other members of the cluster, the updated cluster configuration will be
   * stored on disk, and if the replica crashes and is restarted it will use the persisted configuration rather
   * than the user-provided configuration. This behavior cannot be overridden.
   *
   * @param address The address through which clients and replicas connect to the replica.
   * @param properties The replica properties.
   * @return The replica builder.
   */
  public static Builder builder(Address address, Properties properties) {
    return builder(address, address, properties);
  }

  /**
   * Returns a new Atomix replica builder.
   * <p>
   * The provided server {@link Address} is the address to which the replica will bind for communication with
   * other replicas in the cluster.
   * <p>
   * The client {@link Address} is the address to which clients will connect to the replica to open and close
   * resources and submit state change operations.
   * <p>
   * The provided set of members will be used to connect to the other members in the Raft cluster. The members
   * do not have to be representative of the full cluster membership.
   * <p>
   * When starting a new cluster, the cluster should be formed by providing the {@code members} list of the
   * replicas that make up the initial members of the cluster. If the replica being built is included in the
   * initial membership list, its {@link Address} should be listed in the {@code members} list. Otherwise,
   * if the replica is joining an existing cluster, its {@link Address} should not be listed in the membership
   * list.
   * <p>
   * If the replica uses a persistent {@link io.atomix.copycat.server.storage.StorageLevel} like
   * {@link io.atomix.copycat.server.storage.StorageLevel#DISK DISK} or {@link io.atomix.copycat.server.storage.StorageLevel#MAPPED MAPPED}
   * then the provided membership list only applies to the first time the replica is started. Once the replica
   * has been started and joined with other members of the cluster, the updated cluster configuration will be
   * stored on disk, and if the replica crashes and is restarted it will use the persisted configuration rather
   * than the user-provided configuration. This behavior cannot be overridden.
   *
   * @param clientAddress The address through which clients connect to the replica.
   * @param serverAddress The address through which other replicas connect to the replica.
   * @return The replica builder.
   */
  public static Builder builder(Address clientAddress, Address serverAddress, Properties properties) {
    ServerOptions options = new ServerOptions(properties);
    return new Builder(clientAddress, serverAddress)
      .withTransport(options.transport())
      .withResourceTypes(options.resourceTypes())
      .withStorage(Storage.builder()
        .withStorageLevel(options.storageLevel())
        .withDirectory(options.storageDirectory())
        .withMaxSegmentSize(options.maxSegmentSize())
        .withMaxEntriesPerSegment(options.maxEntriesPerSegment())
        .withRetainStaleSnapshots(options.retainStaleSnapshots())
        .withCompactionThreads(options.compactionThreads())
        .withMinorCompactionInterval(options.minorCompactionInterval())
        .withMajorCompactionInterval(options.majorCompactionInterval())
        .withCompactionThreshold(options.compactionThreshold())
        .build())
      .withSerializer(options.serializer())
      .withElectionTimeout(options.electionTimeout())
      .withHeartbeatInterval(options.heartbeatInterval())
      .withSessionTimeout(options.sessionTimeout());
  }

  private final ResourceServer server;
  private final ClusterManager clusterManager;

  private AtomixReplica(ResourceClient client, ResourceServer server, ClusterManager clusterManager) {
    super(client);
    this.server = Assert.notNull(server, "server");
    this.clusterManager = Assert.notNull(clusterManager, "clusterManager");
  }

  /**
   * Returns the replica type.
   * <p>
   * The replica type defines how the replica behaves within the Atomix cluster. {@link Type#ACTIVE} and
   * {@link Type#PASSIVE} replicas are stateful and participate in replication of state changes within the
   * cluster at different levels. {@link Type#RESERVE} replicas are stateless.
   *
   * @return The replica type.
   */
  public Type type() {
    Member.Type type = server.server().cluster().member().type();
    if (type == null || type == Member.Type.INACTIVE) {
      return null;
    }
    return Type.valueOf(type.name());
  }

  /**
   * Bootstraps a single-node cluster.
   * <p>
   * Bootstrapping a single-node cluster results in the server forming a new cluster to which additional servers
   * can be joined.
   * <p>
   * Only {@link Member.Type#ACTIVE} members can be included in a bootstrap configuration. If the local server is
   * not initialized as an active member, it cannot be part of the bootstrap configuration for the cluster.
   * <p>
   * When the cluster is bootstrapped, the local server will be transitioned into the active state and begin
   * participating in the Raft consensus algorithm. When the cluster is first bootstrapped, no leader will exist.
   * The bootstrapped members will elect a leader amongst themselves. Once a cluster has been bootstrapped, additional
   * members may be {@link #join(Address...) joined} to the cluster. In the event that the bootstrapped members cannot
   * reach a quorum to elect a leader, bootstrap will continue until successful.
   * <p>
   * It is critical that all servers in a bootstrap configuration be started with the same exact set of members.
   * Bootstrapping multiple servers with different configurations may result in split brain.
   * <p>
   * The {@link CompletableFuture} returned by this method will be completed once the cluster has been bootstrapped,
   * a leader has been elected, and the leader has been notified of the local server's client configurations.
   *
   * @return A completable future to be completed once the cluster has been bootstrapped.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomixReplica> bootstrap() {
    return bootstrap(Collections.EMPTY_LIST);
  }

  /**
   * Bootstraps the cluster using the provided cluster configuration.
   * <p>
   * Bootstrapping the cluster results in a new cluster being formed with the provided configuration. The initial
   * nodes in a cluster must always be bootstrapped. This is necessary to prevent split brain. If the provided
   * configuration is empty, the local server will form a single-node cluster.
   * <p>
   * Only {@link Member.Type#ACTIVE} members can be included in a bootstrap configuration. If the local server is
   * not initialized as an active member, it cannot be part of the bootstrap configuration for the cluster.
   * <p>
   * When the cluster is bootstrapped, the local server will be transitioned into the active state and begin
   * participating in the Raft consensus algorithm. When the cluster is first bootstrapped, no leader will exist.
   * The bootstrapped members will elect a leader amongst themselves. Once a cluster has been bootstrapped, additional
   * members may be {@link #join(Address...) joined} to the cluster. In the event that the bootstrapped members cannot
   * reach a quorum to elect a leader, bootstrap will continue until successful.
   * <p>
   * It is critical that all servers in a bootstrap configuration be started with the same exact set of members.
   * Bootstrapping multiple servers with different configurations may result in split brain.
   * <p>
   * The {@link CompletableFuture} returned by this method will be completed once the cluster has been bootstrapped,
   * a leader has been elected, and the leader has been notified of the local server's client configurations.
   *
   * @param cluster The bootstrap cluster configuration.
   * @return A completable future to be completed once the cluster has been bootstrapped.
   */
  public CompletableFuture<AtomixReplica> bootstrap(Address... cluster) {
    return bootstrap(Arrays.asList(cluster));
  }

  /**
   * Bootstraps the cluster using the provided cluster configuration.
   * <p>
   * Bootstrapping the cluster results in a new cluster being formed with the provided configuration. The initial
   * nodes in a cluster must always be bootstrapped. This is necessary to prevent split brain. If the provided
   * configuration is empty, the local server will form a single-node cluster.
   * <p>
   * Only {@link Member.Type#ACTIVE} members can be included in a bootstrap configuration. If the local server is
   * not initialized as an active member, it cannot be part of the bootstrap configuration for the cluster.
   * <p>
   * When the cluster is bootstrapped, the local server will be transitioned into the active state and begin
   * participating in the Raft consensus algorithm. When the cluster is first bootstrapped, no leader will exist.
   * The bootstrapped members will elect a leader amongst themselves. Once a cluster has been bootstrapped, additional
   * members may be {@link #join(Address...) joined} to the cluster. In the event that the bootstrapped members cannot
   * reach a quorum to elect a leader, bootstrap will continue until successful.
   * <p>
   * It is critical that all servers in a bootstrap configuration be started with the same exact set of members.
   * Bootstrapping multiple servers with different configurations may result in split brain.
   * <p>
   * The {@link CompletableFuture} returned by this method will be completed once the cluster has been bootstrapped,
   * a leader has been elected, and the leader has been notified of the local server's client configurations.
   *
   * @param cluster The bootstrap cluster configuration.
   * @return A completable future to be completed once the cluster has been bootstrapped.
   */
  public CompletableFuture<AtomixReplica> bootstrap(Collection<Address> cluster) {
    return server.bootstrap(cluster)
      .thenCompose(v -> clusterManager.start(server.server().cluster(), this))
      .thenCompose(v -> client.connect(cluster))
      .thenApply(v -> this);
  }

  /**
   * Joins the cluster.
   * <p>
   * Joining the cluster results in the local server being added to an existing cluster that has already been
   * bootstrapped. The provided configuration will be used to connect to the existing cluster and submit a join
   * request. Once the server has been added to the existing cluster's configuration, the join operation is complete.
   * <p>
   * Any {@link Member.Type type} of server may join a cluster. In order to join a cluster, the provided list of
   * bootstrapped members must be non-empty and must include at least one active member of the cluster. If no member
   * in the configuration is reachable, the server will continue to attempt to join the cluster until successful. If
   * the provided cluster configuration is empty, the returned {@link CompletableFuture} will be completed exceptionally.
   * <p>
   * When the server joins the cluster, the local server will be transitioned into its initial state as defined by
   * the configured {@link Member.Type}. Once the server has joined, it will immediately begin participating in
   * Raft and asynchronous replication according to its configuration.
   * <p>
   * It's important to note that the provided cluster configuration will only be used the first time the server attempts
   * to join the cluster. Thereafter, in the event that the server crashes and is restarted by {@code join}ing the cluster
   * again, the last known configuration will be used assuming the server is configured with persistent storage. Only when
   * the server leaves the cluster will its configuration and log be reset.
   * <p>
   * In order to preserve safety during configuration changes, Copycat leaders do not allow concurrent configuration
   * changes. In the event that an existing configuration change (a server joining or leaving the cluster or a
   * member being {@link Member#promote() promoted} or {@link Member#demote() demoted}) is under way, the local
   * server will retry attempts to join the cluster until successful. If the server fails to reach the leader,
   * the join will be retried until successful.
   *
   * @param cluster A collection of cluster member addresses to join.
   * @return A completable future to be completed once the local server has joined the cluster.
   */
  public CompletableFuture<AtomixReplica> join(Address... cluster) {
    return join(Arrays.asList(cluster));
  }

  /**
   * Joins the cluster.
   * <p>
   * Joining the cluster results in the local server being added to an existing cluster that has already been
   * bootstrapped. The provided configuration will be used to connect to the existing cluster and submit a join
   * request. Once the server has been added to the existing cluster's configuration, the join operation is complete.
   * <p>
   * Any {@link Member.Type type} of server may join a cluster. In order to join a cluster, the provided list of
   * bootstrapped members must be non-empty and must include at least one active member of the cluster. If no member
   * in the configuration is reachable, the server will continue to attempt to join the cluster until successful. If
   * the provided cluster configuration is empty, the returned {@link CompletableFuture} will be completed exceptionally.
   * <p>
   * When the server joins the cluster, the local server will be transitioned into its initial state as defined by
   * the configured {@link Member.Type}. Once the server has joined, it will immediately begin participating in
   * Raft and asynchronous replication according to its configuration.
   * <p>
   * It's important to note that the provided cluster configuration will only be used the first time the server attempts
   * to join the cluster. Thereafter, in the event that the server crashes and is restarted by {@code join}ing the cluster
   * again, the last known configuration will be used assuming the server is configured with persistent storage. Only when
   * the server leaves the cluster will its configuration and log be reset.
   * <p>
   * In order to preserve safety during configuration changes, Copycat leaders do not allow concurrent configuration
   * changes. In the event that an existing configuration change (a server joining or leaving the cluster or a
   * member being {@link Member#promote() promoted} or {@link Member#demote() demoted}) is under way, the local
   * server will retry attempts to join the cluster until successful. If the server fails to reach the leader,
   * the join will be retried until successful.
   *
   * @param cluster A collection of cluster member addresses to join.
   * @return A completable future to be completed once the local server has joined the cluster.
   */
  public CompletableFuture<AtomixReplica> join(Collection<Address> cluster) {
    return server.join(cluster)
      .thenCompose(v -> clusterManager.start(server.server().cluster(), this))
      .thenCompose(v -> client.connect(cluster))
      .thenApply(v -> this);
  }

  /**
   * Shuts down the server without leaving the Copycat cluster.
   *
   * @return A completable future to be completed once the server has been shutdown.
   */
  public CompletableFuture<Void> shutdown() {
    return clusterManager.stop(server.server().cluster(), this)
      .thenCompose(v -> client.close())
      .thenCompose(v -> server.shutdown());
  }

  /**
   * Leaves the Copycat cluster.
   *
   * @return A completable future to be completed once the server has left the cluster.
   */
  public CompletableFuture<Void> leave() {
    return clusterManager.stop(server.server().cluster(), this)
      .thenCompose(v -> client.close())
      .thenCompose(v -> server.leave());
  }

  /**
   * Builder for programmatically constructing an {@link AtomixReplica}.
   * <p>
   * The replica builder configures an {@link AtomixReplica} to listen for connections from clients and other
   * servers/replica, connect to other servers in a cluster, and manage a replicated log. To create a replica builder,
   * use the {@link #builder(Address)} method:
   * <pre>
   *   {@code
   *   Atomix replica = AtomixReplica.builder(address, members)
   *     .withTransport(new NettyTransport())
   *     .withStorage(Storage.builder()
   *       .withDirectory("logs")
   *       .withStorageLevel(StorageLevel.MAPPED)
   *       .build())
   *     .build();
   *   }
   * </pre>
   * The two most essential components of the builder are the {@link Transport} and {@link Storage}. The
   * transport provides the mechanism for the replica to communicate with clients and other replicas in the
   * cluster. All servers, clients, and replicas must implement the same {@link Transport} type. The {@link Storage}
   * module configures how the replica manages the replicated log. Logs can be written to disk or held in
   * memory or memory-mapped files.
   */
  public static class Builder implements io.atomix.catalyst.util.Builder<AtomixReplica> {
    private final Address clientAddress;
    private final CopycatClient.Builder clientBuilder;
    private final CopycatServer.Builder serverBuilder;
    private final ResourceRegistry registry = new ResourceRegistry(RESOURCES);
    private Transport clientTransport;
    private Transport serverTransport;
    private ClusterManager clusterManager;
    private LocalServerRegistry localRegistry = new LocalServerRegistry();

    private Builder(Address clientAddress, Address serverAddress) {
      Serializer serializer = new Serializer();
      this.clientAddress = Assert.notNull(clientAddress, "clientAddress");
      this.clientBuilder = CopycatClient.builder()
        .withSerializer(serializer.clone())
        .withServerSelectionStrategy(ServerSelectionStrategies.ANY)
        .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
        .withRecoveryStrategy(RecoveryStrategies.RECOVER);
      this.serverBuilder = CopycatServer.builder(clientAddress, serverAddress).withSerializer(serializer.clone());
    }

    /**
     * Sets the server member type.
     *
     * @param type The server member type.
     * @return The replica builder.
     */
    public Builder withType(Type type) {
      serverBuilder.withType(Member.Type.valueOf(Assert.notNull(type, "type").name()));
      return this;
    }

    /**
     * Sets the replica transport, returning the replica builder for method chaining.
     * <p>
     * The configured transport should be the same transport as all other nodes in the cluster.
     * If no transport is explicitly provided, the instance will default to the {@code NettyTransport}
     * if available on the classpath.
     *
     * @param transport The replica transport.
     * @return The replica builder.
     * @throws NullPointerException if {@code transport} is null
     */
    public Builder withTransport(Transport transport) {
      this.serverTransport = Assert.notNull(transport, "transport");
      return this;
    }

    /**
     * Sets the client transport, returning the server builder for method chaining.
     * <p>
     * The configured transport should be the same transport as all clients.
     * If no transport is explicitly provided, the instance will default to the {@code NettyTransport}
     * if available on the classpath.
     *
     * @param transport The server transport.
     * @return The server builder.
     * @throws NullPointerException if {@code transport} is null
     */
    public Builder withClientTransport(Transport transport) {
      this.clientTransport = Assert.notNull(transport, "transport");
      return this;
    }

    /**
     * Sets the server transport, returning the server builder for method chaining.
     * <p>
     * The configured transport should be the same transport as all other servers in the cluster.
     * If no transport is explicitly provided, the instance will default to the {@code NettyTransport}
     * if available on the classpath.
     *
     * @param transport The server transport.
     * @return The server builder.
     * @throws NullPointerException if {@code transport} is null
     */
    public Builder withServerTransport(Transport transport) {
      this.serverTransport = Assert.notNull(transport, "transport");
      return this;
    }

    /**
     * Sets the serializer, returning the replica builder for method chaining.
     * <p>
     * The serializer will be used to serialize and deserialize operations that are sent over the wire.
     *
     * @param serializer The serializer.
     * @return The replica builder.
     * @throws NullPointerException if {@code serializer} is null
     */
    public Builder withSerializer(Serializer serializer) {
      clientBuilder.withSerializer(serializer);
      serverBuilder.withSerializer(serializer);
      return this;
    }

    /**
     * Sets the replica cluster manager.
     *
     * @param clusterManager The replica cluster manager.
     * @return The replica builder.
     */
    public Builder withClusterManager(ClusterManager clusterManager) {
      this.clusterManager = Assert.notNull(clusterManager, "clusterManager");
      return this;
    }

    /**
     * Sets the replica storage module, returning the replica builder for method chaining.
     * <p>
     * The storage module is the interface the replica will use to store the persistent replicated log.
     * For simple configurations, users can simply construct a {@link Storage} object:
     * <pre>
     *   {@code
     *   Atomix replica = AtomixReplica.builder(address, members)
     *     .withStorage(new Storage("logs"))
     *     .build();
     *   }
     * </pre>
     * Users can configure how state changes are stored on the replica by setting the
     * {@link io.atomix.copycat.server.storage.StorageLevel}. Use the
     * {@link io.atomix.copycat.server.storage.Storage.Builder} for the greatest flexibility in configuring
     * the replica's storage layer:
     * <pre>
     *   {@code
     *   Atomix replica = AtomixReplica.builder(address, members)
     *     .withStorage(Storage.builder()
     *       .withDirectory("logs")
     *       .withStorageLevel(StorageLevel.MAPPED)
     *       .withCompactionThreads(2)
     *       .build())
     *     .build();
     *   }
     * </pre>
     * For the greatest safety, it's recommended that users use the
     * {@link io.atomix.copycat.server.storage.StorageLevel#DISK DISK} storage level. Alternatively, the
     * {@link io.atomix.copycat.server.storage.StorageLevel#MAPPED MAPPED} storage level provides significantly
     * greater performance without significantly relaxing safety. The {@link io.atomix.copycat.server.storage.StorageLevel#MEMORY MEMORY}
     * storage level is not recommended for production. For replicas that use the {@code MEMORY} storage level,
     * Atomix cannot guarantee writes will not be lost of a majority of the cluster is lost.
     *
     * @param storage The replica storage module.
     * @return The replica builder.
     * @throws NullPointerException if {@code storage} is null
     */
    public Builder withStorage(Storage storage) {
      serverBuilder.withStorage(storage);
      return this;
    }

    /**
     * Sets the replica election timeout, returning the replica builder for method chaining.
     * <p>
     * The election timeout is the duration since last contact with the cluster leader after which
     * the replica should start a new election. The election timeout should always be significantly
     * larger than {@link #withHeartbeatInterval(Duration)} in order to prevent unnecessary elections.
     *
     * @param electionTimeout The replica election timeout.
     * @return The replica builder.
     * @throws NullPointerException if {@code electionTimeout} is null
     */
    public Builder withElectionTimeout(Duration electionTimeout) {
      serverBuilder.withElectionTimeout(electionTimeout);
      return this;
    }

    /**
     * Sets the replica heartbeat interval, returning the replica builder for method chaining.
     * <p>
     * The heartbeat interval is the interval at which the replica, if elected leader, should contact
     * other replicas within the cluster to maintain its leadership. The heartbeat interval should
     * always be some fraction of {@link #withElectionTimeout(Duration)}.
     *
     * @param heartbeatInterval The replica heartbeat interval.
     * @return The replica builder.
     * @throws NullPointerException if {@code heartbeatInterval} is null
     */
    public Builder withHeartbeatInterval(Duration heartbeatInterval) {
      serverBuilder.withHeartbeatInterval(heartbeatInterval);
      return this;
    }

    /**
     * Sets the replica session timeout, returning the replica builder for method chaining.
     * <p>
     * The session timeout is assigned by the replica to a client which opens a new session. The session timeout
     * dictates the interval at which the client must send keep-alive requests to the cluster to maintain its
     * session. If a client fails to communicate with the cluster for larger than the configured session
     * timeout, its session may be expired.
     *
     * @param sessionTimeout The replica session timeout.
     * @return The replica builder.
     * @throws NullPointerException if {@code sessionTimeout} is null
     */
    public Builder withSessionTimeout(Duration sessionTimeout) {
      serverBuilder.withSessionTimeout(sessionTimeout);
      return this;
    }

    /**
     * Sets the replica's global suspend timeout.
     * <p>
     * The global suspend timeout is an advanced configuration option that controls how long a leader
     * waits for a partitioned follower to rejoin the cluster before forcing that follower to truncate
     * its logs. Because of various consistency issues, followers must be forced to truncate their logs
     * after crashing or being partitioned for a lengthy amount of time in order to allow the cluster to
     * progress. Specifically, in Atomix while a follower is partitioned replicas cannot compact their
     * logs of certain types of commits like resource deletes and other tombstones.
     * <p>
     * By default, replicas will wait at least an hour for a partitioned follower to rejoin the cluster
     * before advancing compaction and forcing the follower to truncate its logs once the partition heals.
     * However, for clusters that have a surplus of replicas, Atomix may replace a partitioned follower
     * with a connected follower anyways, so this option frequently does not apply to larger clusters.
     *
     * @param globalSuspendTimeout The global suspend timeout.
     * @return The replica builder.
     * @throws NullPointerException if {@code globalSuspendTimeout} is null
     */
    public Builder withGlobalSuspendTimeout(Duration globalSuspendTimeout) {
      serverBuilder.withGlobalSuspendTimeout(globalSuspendTimeout);
      return this;
    }

    /**
     * Sets the available resource types.
     *
     * @param types The available resource types.
     * @return The replica builder.
     */
    public Builder withResourceTypes(Class<? extends Resource<?>>... types) {
      if (types != null) {
        return withResourceTypes(Arrays.asList(types).stream().map(ResourceType::new).collect(Collectors.toList()));
      }
      return this;
    }

    /**
     * Sets the available resource types.
     *
     * @param types The available resource types.
     * @return The replica builder.
     */
    public Builder withResourceTypes(ResourceType... types) {
      if (types != null) {
        return withResourceTypes(Arrays.asList(types));
      }
      return this;
    }

    /**
     * Sets the available resource types.
     *
     * @param types The available resource types.
     * @return The replica builder.
     */
    public Builder withResourceTypes(Collection<ResourceType> types) {
      types.forEach(registry::register);
      return this;
    }

    /**
     * Adds a resource type to the replica.
     *
     * @param type The resource type.
     * @return The replica builder.
     */
    public Builder addResourceType(Class<? extends Resource<?>> type) {
      return addResourceType(new ResourceType(type));
    }

    /**
     * Adds a resource type to the replica.
     *
     * @param type The resource type.
     * @return The replica builder.
     */
    public Builder addResourceType(ResourceType type) {
      registry.register(type);
      return this;
    }

    /**
     * Builds the replica.
     * <p>
     * If no {@link Transport} was configured for the replica, the builder will attempt to create a
     * {@code NettyTransport} instance. If {@code io.atomix.catalyst.transport.NettyTransport} is not available
     * on the classpath, a {@link ConfigurationException} will be thrown.
     * <p>
     * Once the replica is built, it is not yet connected to the cluster. To connect the replica to the cluster,
     * call the asynchronous {@link #bootstrap()} method.
     *
     * @return The built replica.
     * @throws ConfigurationException if the replica is misconfigured
     */
    @Override
    public AtomixReplica build() {
      // If no transport was configured by the user, attempt to load the Netty transport.
      if (serverTransport == null) {
        try {
          serverTransport = (Transport) Class.forName("io.atomix.catalyst.transport.NettyTransport").newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
          throw new ConfigurationException("transport not configured");
        }
      }

      // Configure the client and server with a transport that routes all local client communication
      // directly through the local server, ensuring we don't incur unnecessary network traffic by
      // sending operations to a remote server when a local server is already available in the same JVM.=
      clientBuilder.withTransport(new CombinedClientTransport(clientAddress, new LocalTransport(localRegistry), clientTransport != null ? clientTransport : serverTransport))
        .withServerSelectionStrategy(new CombinedSelectionStrategy(clientAddress));

      CopycatClient client = clientBuilder.build();
      client.serializer().resolve(new ResourceManagerTypeResolver());

      // Iterate through registered resource types and register serializable types on the client serializer.
      for (ResourceType type : registry.types()) {
        try {
          type.factory().newInstance().createSerializableTypeResolver().resolve(client.serializer().registry());
        } catch (InstantiationException | IllegalAccessException e) {
          throw new ResourceManagerException(e);
        }
      }

      // Create a default cluster manager if none was specified.
      ClusterManager clusterManager = this.clusterManager != null ? this.clusterManager : new ClusterManager() {
        @Override
        public CompletableFuture<Void> start(Cluster cluster, AtomixReplica replica) {
          return CompletableFuture.completedFuture(null);
        }
        @Override
        public CompletableFuture<Void> stop(Cluster cluster, AtomixReplica replica) {
          return CompletableFuture.completedFuture(null);
        }
      };

      // Construct the underlying CopycatServer. The server should have been configured with a CombinedTransport
      // that facilitates the local client connecting directly to the server.
      if (clientTransport != null) {
        serverBuilder.withClientTransport(new CombinedServerTransport(new LocalTransport(localRegistry), clientTransport))
          .withServerTransport(serverTransport);
      } else {
        serverBuilder.withTransport(new CombinedServerTransport(new LocalTransport(localRegistry), serverTransport));
      }

      // Set the server resource state machine.
      serverBuilder.withStateMachine(ResourceManagerState::new);

      CopycatServer server = serverBuilder.build();
      server.serializer().resolve(new ResourceManagerTypeResolver());

      // Iterate through registered resource types and register serializable types on the server serializer.
      for (ResourceType type : registry.types()) {
        try {
          type.factory().newInstance().createSerializableTypeResolver().resolve(server.serializer().registry());
        } catch (InstantiationException | IllegalAccessException e) {
          throw new ResourceManagerException(e);
        }
      }

      return new AtomixReplica(new ResourceClient(new CombinedCopycatClient(client, serverTransport)), new ResourceServer(server), clusterManager);
    }
  }

  /**
   * Copycat client wrapper.
   */
  private static final class CombinedCopycatClient implements CopycatClient {
    private final CopycatClient client;
    private final Transport transport;

    CombinedCopycatClient(CopycatClient client, Transport transport) {
      this.client = Assert.notNull(client, "client");
      this.transport = Assert.notNull(transport, "transport");
    }

    @Override
    public State state() {
      return client.state();
    }

    @Override
    public Listener<State> onStateChange(Consumer<State> consumer) {
      return client.onStateChange(consumer);
    }

    @Override
    public ThreadContext context() {
      return client.context();
    }

    @Override
    public Transport transport() {
      return transport;
    }

    @Override
    public Serializer serializer() {
      return client.serializer();
    }

    @Override
    public Session session() {
      return client.session();
    }

    @Override
    public <T> CompletableFuture<T> submit(Command<T> command) {
      return client.submit(command);
    }

    @Override
    public <T> CompletableFuture<T> submit(Query<T> query) {
      return client.submit(query);
    }

    @Override
    public Listener<Void> onEvent(String event, Runnable callback) {
      return client.onEvent(event, callback);
    }

    @Override
    public <T> Listener<T> onEvent(String event, Consumer<T> callback) {
      return client.onEvent(event, callback);
    }

    @Override
    public CompletableFuture<CopycatClient> connect(Collection<Address> members) {
      return client.connect(members);
    }

    @Override
    public CompletableFuture<CopycatClient> recover() {
      return client.recover();
    }

    @Override
    public CompletableFuture<Void> close() {
      return client.close();
    }

    @Override
    public String toString() {
      return client.toString();
    }

  }

  /**
   * Combined server selection strategy.
   */
  private static class CombinedSelectionStrategy implements ServerSelectionStrategy {
    private final Address address;

    private CombinedSelectionStrategy(Address address) {
      this.address = address;
    }

    @Override
    public List<Address> selectConnections(Address leader, List<Address> servers) {
      List<Address> addresses = new ArrayList<>(servers.size());
      addresses.add(address);
      Collections.shuffle(servers);
      for (Address address : servers) {
        if (!address.equals(this.address)) {
          addresses.add(address);
        }
      }
      return addresses;
    }
  }

  /**
   * Combined client transport.
   */
  private static class CombinedClientTransport implements Transport {
    private final Address address;
    private final Transport local;
    private final Transport remote;

    private CombinedClientTransport(Address address, Transport local, Transport remote) {
      this.address = address;
      this.local = local;
      this.remote = remote;
    }

    @Override
    public Client client() {
      return new CombinedClient(address, local.client(), remote.client());
    }

    @Override
    public Server server() {
      return remote.server();
    }
  }

  /**
   * Combined client,
   */
  private static class CombinedClient implements Client {
    private final Address address;
    private final Client local;
    private final Client remote;

    private CombinedClient(Address address, Client local, Client remote) {
      this.address = address;
      this.local = local;
      this.remote = remote;
    }

    @Override
    public CompletableFuture<Connection> connect(Address address) {
      if (this.address.equals(address)) {
        return local.connect(address);
      }
      return remote.connect(address);
    }

    @Override
    public CompletableFuture<Void> close() {
      return remote.close().thenRun(local::close);
    }
  }

  /**
   * Combined transport that aids in the local client communicating directly with the local server.
   */
  private static class CombinedServerTransport implements Transport {
    private final Transport local;
    private final Transport remote;

    private CombinedServerTransport(Transport local, Transport remote) {
      this.local = local;
      this.remote = remote;
    }

    @Override
    public Client client() {
      return remote.client();
    }

    @Override
    public Server server() {
      return new CombinedServer(local.server(), remote.server());
    }
  }

  /**
   * Combined server that access connections from the local client directly.
   */
  private static class CombinedServer implements Server {
    private final Server local;
    private final Server remote;

    private CombinedServer(Server local, Server remote) {
      this.local = local;
      this.remote = remote;
    }

    @Override
    public CompletableFuture<Void> listen(Address address, Consumer<Connection> listener) {
      Assert.notNull(address, "address");
      Assert.notNull(listener, "listener");
      return local.listen(address, listener).thenCompose(v -> remote.listen(address, listener));
    }

    @Override
    public CompletableFuture<Void> close() {
      return local.close().thenCompose(v -> remote.close());
    }
  }

}
