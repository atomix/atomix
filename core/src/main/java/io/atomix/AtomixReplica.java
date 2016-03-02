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
import io.atomix.catalyst.util.PropertiesReader;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.coordination.DistributedLock;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.session.Session;
import io.atomix.manager.ResourceClient;
import io.atomix.manager.ResourceServer;
import io.atomix.manager.state.ResourceManagerState;
import io.atomix.manager.util.ResourceManagerTypeResolver;
import io.atomix.util.ClusterBalancer;
import io.atomix.util.ReplicaProperties;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Provides an interface for creating and operating on {@link io.atomix.resource.Resource}s as a stateful node.
 * <p>
 * Replicas serve as a hybrid {@link AtomixClient} and server to allow a server to be embedded
 * in an application. From the perspective of state, replicas behave like servers in that they
 * maintain a replicated state machine for {@link io.atomix.resource.Resource}s and fully participate in the underlying
 * consensus algorithm. From the perspective of resources, replicas behave like {@link AtomixClient}s in that
 * they may themselves create and modify distributed resources.
 * <p>
 * To create a replica, use the {@link #builder(Address, Address...)} builder factory. Each replica must
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
 * <h2>Cluster management</h2>
 * Atomix clusters are managed by a custom implementation of the <a href="https://raft.github.io/">Raft consensus algorithm</a>.
 * Raft is a leader-based system that requires a quorum of nodes to synchronously acknowledge a write. Typically,
 * Raft clusters consist of {@code 3} or {@code 5} nodes.
 * <p>
 * But Atomix is designed to be embedded and scale in clusters much larger than the typical {@code 3} or {@code 5}
 * node consensus based cluster. To do so, Atomix assigns a portion of the cluster to participate in the Raft
 * algorithm, and the remainder of the replicas participate in asynchronous replication or await failures as standby
 * nodes. A typical Atomix cluster may consist of {@code 3} active Raft-voting replicas, {@code 2} backup replicas,
 * and any number of reserve nodes. As replicas are added to or removed from the cluster, Atomix transparently
 * balances the cluster to ensure it maintains the desired number of active and backup replicas.
 * <p>
 * Users are responsible for configuring the desired number of Raft participants in the cluster by setting the
 * {@link Builder#withQuorumHint(int) quorumHint}. The quorum hint indicates the desired minimum number of active
 * Raft participants in the cluster. If the cluster consists of enough replicas, Atomix will guarantee that the
 * cluster always has <em>at least</em> {@code quorumHint} replicas.
 * <p>
 * The size of the quorum is relevant both to performance and fault-tolerance. When resources are
 * created or deleted or resource state changes are submitted to the cluster, Atomix will synchronously
 * replicate changes to a majority of the cluster before they can be committed and update state. For
 * example, in a cluster where the {@code quorumHint} is {@code 3}, a
 * {@link io.atomix.collections.DistributedMap#put(Object, Object)} command must be sent to the leader
 * and then synchronously replicated to one other replica before it can be committed and applied to the
 * map state machine. This also means that a cluster with {@code quorumHint} equal to {@code 3} can tolerate
 * at most one failure.
 * <p>
 * Users should set the {@code quorumHint} to an odd number of replicas or use one of the {@link Quorum}
 * magic constants for the greatest level of fault tolerance. By default, the quorum hint is set to
 * {@link Quorum#SEED} which is based on the number of member nodes provided to the replica builder
 * {@link #builder(Address, Address...) factory}. Typically, in write-heavy workloads, the most
 * performant configuration will be a {@code quorumHint} of {@code 3}. In read-heavy workloads, quorum hints
 * of {@code 3} or {@code 5} can be used depending on the size of the cluster and desired level of fault tolerance.
 * Additional active replicas may or may not improve read performance depending on usage and in particular
 * {@link io.atomix.resource.ReadConsistency read consistency} levels.
 * <p>
 * In addition to the {@code quorumHint}, users can also configure the {@code backupCount} to specify the
 * desired number of backup replicas to maintain per active replica. The {@code backupCount} specifies the
 * maximum number of replicas per {@link Builder#withQuorumHint(int) active} replica to participate in
 * asynchronous replication of state. Backup replicas allow quorum-member replicas to be more quickly replaced
 * in the event of a failure or an active replica leaving the cluster. Additionally, backup replicas may
 * service {@link io.atomix.resource.ReadConsistency#SEQUENTIAL SEQUENTIAL} and
 * {@link io.atomix.resource.ReadConsistency#CAUSAL CAUSAL} reads to allow read operations to be further
 * spread across the cluster.
 * <pre>
 *   {@code
 *   Atomix atomix = AtomixReplica.builder(address, members)
 *     .withTransport(new NettyTransport())
 *     .withStorage(new Storage(StorageLevel.MEMORY))
 *     .withQuorumHint(3)
 *     .withBackupCount(1)
 *     .build();
 *   }
 * </pre>
 * The backup count is used to calculate the number of backup replicas per non-leader active member. The
 * number of actual backups is calculated by {@code (quorumHint - 1) * backupCount}. If the
 * {@code backupCount} is {@code 1} and the {@code quorumHint} is {@code 3}, the number of backup replicas
 * will be {@code 2}.
 * <p>
 * By default, the backup count is {@code 0}, indicating no backups should be maintained. However, it is
 * recommended that each cluster have at least a backup count of {@code 1} to ensure active replicas can
 * be quickly replaced in the event of a network partition or other failure. Quick replacement of active
 * member nodes improves fault tolerance in cases where a majority of the active members in the cluster are
 * not lost simultaneously.
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
 * When the replica is {@link #open() started}, the replica will attempt to contact members in the configured
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
 * provided to the {@link #builder(Address, Address...) builder factory}. This allows Atomix cluster structures
 * to change transparently and independently of the code that configures any given replica. If a persistent
 * {@link io.atomix.copycat.server.storage.StorageLevel} is used, user code should simply configure the replica
 * consistently based on the <em>initial</em> replica configuration, and the replica will recover from the last
 * known cluster configuration in the event of a failure.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class AtomixReplica extends Atomix {

  /**
   * Returns a new Atomix replica builder from the given configuration file.
   *
   * @param properties The properties file from which to load the replica builder.
   * @return The replica builder.
   */
  public static Builder builder(String properties) {
    return builder(PropertiesReader.load(properties).properties());
  }

  /**
   * Returns a new Atomix replica builder from the given properties.
   *
   * @param properties The properties from which to load the replica builder.
   * @return The replica builder.
   */
  public static Builder builder(Properties properties) {
    ReplicaProperties replicaProperties = new ReplicaProperties(properties);
    Collection<Address> replicas = replicaProperties.replicas();
    return builder(replicaProperties.clientAddress(), replicaProperties.serverAddress(), replicas)
      .withTransport(replicaProperties.transport())
      .withStorage(Storage.builder()
        .withStorageLevel(replicaProperties.storageLevel())
        .withDirectory(replicaProperties.storageDirectory())
        .withMaxSegmentSize(replicaProperties.maxSegmentSize())
        .withMaxEntriesPerSegment(replicaProperties.maxEntriesPerSegment())
        .withMaxSnapshotSize(replicaProperties.maxSnapshotSize())
        .withRetainStaleSnapshots(replicaProperties.retainStaleSnapshots())
        .withCompactionThreads(replicaProperties.compactionThreads())
        .withMinorCompactionInterval(replicaProperties.minorCompactionInterval())
        .withMajorCompactionInterval(replicaProperties.majorCompactionInterval())
        .withCompactionThreshold(replicaProperties.compactionThreshold())
        .build())
      .withSerializer(replicaProperties.serializer())
      .withQuorumHint(replicaProperties.quorumHint() != -1 ? replicaProperties.quorumHint() : replicas.size())
      .withBackupCount(replicaProperties.backupCount())
      .withElectionTimeout(replicaProperties.electionTimeout())
      .withHeartbeatInterval(replicaProperties.heartbeatInterval())
      .withSessionTimeout(replicaProperties.sessionTimeout());
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
   * @param members The cluster members to which to connect.
   * @return The replica builder.
   */
  public static Builder builder(Address address, Address... members) {
    return builder(address, address, Arrays.asList(Assert.notNull(members, "members")));
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
   * @param members The cluster members to which to connect.
   * @return The replica builder.
   */
  public static Builder builder(Address address, Collection<Address> members) {
    return new Builder(address, address, members);
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
   * @param members The cluster members to which to connect.
   * @return The replica builder.
   */
  public static Builder builder(Address clientAddress, Address serverAddress, Address... members) {
    return builder(clientAddress, serverAddress, Arrays.asList(Assert.notNull(members, "members")));
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
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The local server member address.
   * @param members The cluster members to which to connect.
   * @return The replica builder.
   */
  public static Builder builder(Address clientAddress, Address serverAddress, Collection<Address> members) {
    return new Builder(clientAddress, serverAddress, members);
  }

  private final ResourceServer server;
  private final ClusterBalancer balancer;
  private DistributedLock lock;
  private boolean locking;

  public AtomixReplica(Properties properties) {
    this(builder(properties));
  }

  private AtomixReplica(Builder builder) {
    this(builder.buildClient(), builder.buildServer(), builder.buildBalancer());
  }

  private AtomixReplica(ResourceClient client, ResourceServer server, ClusterBalancer balancer) {
    super(client);
    this.server = Assert.notNull(server, "server");
    this.balancer = Assert.notNull(balancer, "balancer");
  }

  /**
   * Registers membership change listeners on the cluster.
   */
  private void registerListeners() {
    server.server().cluster().members().forEach(m -> {
      m.onTypeChange(t -> balance());
      m.onStatusChange(s -> balance());
    });

    server.server().cluster().onLeaderElection(l -> balance());

    server.server().cluster().onJoin(m -> {
      m.onTypeChange(t -> balance());
      m.onStatusChange(s -> balance());
      balance();
    });

    server.server().cluster().onLeave(m -> balance());
  }

  /**
   * Balances the cluster.
   */
  private void balance() {
    if (lock != null && !locking && server.server().cluster().member().equals(server.server().cluster().leader())) {
      locking = true;
      lock.lock()
        .thenCompose(v -> balancer.balance(server.server().cluster()))
        .whenComplete((r1, e1) -> lock.unlock().whenComplete((r2, e2) -> locking = false));
    }
  }

  @Override
  public CompletableFuture<Atomix> open() {
    return server.open()
      .thenRun(this::registerListeners)
      .thenCompose(v -> super.open())
      .thenCompose(v -> client.get("", DistributedLock.class))
      .thenApply(lock -> {
        this.lock = lock;
        return this;
      });
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    lock.lock()
      .thenCompose(v -> balancer.replace(server.server().cluster()))
      .whenComplete((r1, e1) -> {
        balancer.close();
        lock.unlock().whenComplete((r2, e2) -> {
          super.close().whenComplete((r3, e3) -> {
            server.close().whenComplete((r4, e4) -> {
              if (e4 == null) {
                future.complete(null);
              } else {
                future.completeExceptionally(e4);
              }
            });
          });
        });
      });
    return future;
  }

  /**
   * Builder for programmatically constructing an {@link AtomixReplica}.
   * <p>
   * The replica builder configures an {@link AtomixReplica} to listen for connections from clients and other
   * servers/replica, connect to other servers in a cluster, and manage a replicated log. To create a replica builder,
   * use the {@link #builder(Address, Address...)} method:
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
    private Transport clientTransport;
    private Transport serverTransport;
    private final Collection<Address> members;
    private int quorumHint;
    private int backupCount;
    private LocalServerRegistry localRegistry = new LocalServerRegistry();

    private Builder(Address clientAddress, Address serverAddress, Collection<Address> members) {
      this.members = Assert.notNull(members, "members");
      Serializer serializer = new Serializer();
      this.clientAddress = Assert.notNull(clientAddress, "clientAddress");
      this.clientBuilder = CopycatClient.builder(members)
        .withSerializer(serializer.clone())
        .withServerSelectionStrategy(ServerSelectionStrategies.ANY)
        .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
        .withRecoveryStrategy(RecoveryStrategies.RECOVER)
        .withRetryStrategy(RetryStrategies.FIBONACCI_BACKOFF);
      this.serverBuilder = CopycatServer.builder(clientAddress, serverAddress, members).withSerializer(serializer.clone());
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
     * Sets the cluster quorum hint.
     * <p>
     * The quorum hint specifies the optimal number of replicas to actively participate in the Raft
     * consensus algorithm. As long as there are at least {@code quorumHint} replicas in the cluster,
     * Atomix will automatically balance replicas to ensure that at least {@code quorumHint} replicas
     * are active participants in the Raft algorithm at any given time. Replicas can be added to or
     * removed from the cluster at will, and remaining replicas will be transparently promoted and demoted
     * as necessary to maintain the desired quorum size.
     * <p>
     * By default, the configured quorum hint is {@link Quorum#SEED} which is based on the number of
     * {@link Address members} provided to the {@link #builder(Address, Address...) builder factory}
     * when constructing this builder.
     * <p>
     * The size of the quorum is relevant both to performance and fault-tolerance. When resources are
     * created or deleted or resource state changes are submitted to the cluster, Atomix will synchronously
     * replicate changes to a majority of the cluster before they can be committed and update state. For
     * example, in a cluster where the {@code quorumHint} is {@code 3}, a
     * {@link io.atomix.collections.DistributedMap#put(Object, Object)} command must be sent to the leader
     * and then synchronously replicated to one other replica before it can be committed and applied to the
     * map state machine. This also means that a cluster with {@code quorumHint} equal to {@code 3} can tolerate
     * at most one failure.
     * <p>
     * Users should set the {@code quorumHint} to an odd number of replicas or use one of the {@link Quorum}
     * magic constants for the greatest level of fault tolerance. Typically, in write-heavy workloads, the most
     * performant configuration will be a {@code quorumHint} of {@code 3}. In read-heavy workloads, quorum hints
     * of {@code 3} or {@code 5} can be used depending on the size of the cluster and desired level of fault tolerance.
     * Additional active replicas may or may not improve read performance depending on usage and in particular
     * {@link io.atomix.resource.ReadConsistency read consistency} levels.
     *
     * @param quorumHint The quorum hint. This must be the same on all replicas in the cluster.
     * @return The replica builder.
     * @throws IllegalArgumentException if the quorum hint is less than {@code -1}
     */
    public Builder withQuorumHint(int quorumHint) {
      this.quorumHint = Assert.argNot(quorumHint, quorumHint < -1, "quorumHint must be positive, 0, or -1");
      return this;
    }

    /**
     * Sets the cluster quorum hint.
     * <p>
     * The quorum hint specifies the optimal number of replicas to actively participate in the Raft
     * consensus algorithm. As long as there are at least {@code quorumHint} replicas in the cluster,
     * Atomix will automatically balance replicas to ensure that at least {@code quorumHint} replicas
     * are active participants in the Raft algorithm at any given time. Replicas can be added to or
     * removed from the cluster at will, and remaining replicas will be transparently promoted and demoted
     * as necessary to maintain the desired quorum size.
     * <p>
     * By default, the configured quorum hint is {@link Quorum#SEED} which is based on the number of
     * {@link Address members} provided to the {@link #builder(Address, Address...) builder factory}
     * when constructing this builder.
     * <p>
     * The size of the quorum is relevant both to performance and fault-tolerance. When resources are
     * created or deleted or resource state changes are submitted to the cluster, Atomix will synchronously
     * replicate changes to a majority of the cluster before they can be committed and update state. For
     * example, in a cluster where the {@code quorumHint} is {@code 3}, a
     * {@link io.atomix.collections.DistributedMap#put(Object, Object)} command must be sent to the leader
     * and then synchronously replicated to one other replica before it can be committed and applied to the
     * map state machine. This also means that a cluster with {@code quorumHint} equal to {@code 3} can tolerate
     * at most one failure.
     * <p>
     * Users should set the {@code quorumHint} to an odd number of replicas or use one of the {@link Quorum}
     * magic constants for the greatest level of fault tolerance. Typically, in write-heavy workloads, the most
     * performant configuration will be a {@code quorumHint} of {@code 3}. In read-heavy workloads, quorum hints
     * of {@code 3} or {@code 5} can be used depending on the size of the cluster and desired level of fault tolerance.
     * Additional active replicas may or may not improve read performance depending on usage and in particular
     * {@link io.atomix.resource.ReadConsistency read consistency} levels.
     *
     * @param quorum The quorum hint. This must be the same on all replicas in the cluster.
     * @return The replica builder.
     * @throws NullPointerException if the quorum hint is null
     */
    public Builder withQuorumHint(Quorum quorum) {
      this.quorumHint = Assert.notNull(quorum, "quorum").size();
      return this;
    }

    /**
     * Sets the replica backup count.
     * <p>
     * The backup count specifies the maximum number of replicas per {@link #withQuorumHint(int) active} replica
     * to participate in asynchronous replication of state. Backup replicas allow quorum-member replicas to be
     * more quickly replaced in the event of a failure or an active replica leaving the cluster. Additionally,
     * backup replicas may service {@link io.atomix.resource.ReadConsistency#SEQUENTIAL SEQUENTIAL} and
     * {@link io.atomix.resource.ReadConsistency#CAUSAL CAUSAL} reads to allow read operations to be further
     * spread across the cluster.
     * <p>
     * The backup count is used to calculate the number of backup replicas per non-leader active member. The
     * number of actual backups is calculated by {@code (quorumHint - 1) * backupCount}. If the
     * {@code backupCount} is {@code 1} and the {@code quorumHint} is {@code 3}, the number of backup replicas
     * will be {@code 2}.
     * <p>
     * By default, the backup count is {@code 0}, indicating no backups should be maintained. However, it is
     * recommended that each cluster have at least a backup count of {@code 1} to ensure active replicas can
     * be quickly replaced in the event of a network partition or other failure. Quick replacement of active
     * member nodes improves fault tolerance in cases where a majority of the active members in the cluster are
     * not lost simultaneously.
     *
     * @param backupCount The number of backup replicas per active replica. This must be the same on all
     *                    replicas in the cluster.
     * @return The replica builder.
     * @throws IllegalArgumentException if the {@code backupCount} is negative
     */
    public Builder withBackupCount(int backupCount) {
      this.backupCount = Assert.argNot(backupCount, backupCount < 0, "backupCount must be positive");
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
     * Builds the replica transports.
     */
    private void buildTransport() {
      // If no transport was configured by the user, attempt to load the Netty transport.
      if (serverTransport == null) {
        try {
          serverTransport = (Transport) Class.forName("io.atomix.catalyst.transport.NettyTransport").newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
          throw new ConfigurationException("transport not configured");
        }
      }
    }

    /**
     * Builds a client for the replica.
     */
    private ResourceClient buildClient() {
      buildTransport();

      // Configure the client and server with a transport that routes all local client communication
      // directly through the local server, ensuring we don't incur unnecessary network traffic by
      // sending operations to a remote server when a local server is already available in the same JVM.=
      clientBuilder.withTransport(new CombinedClientTransport(clientAddress, new LocalTransport(localRegistry), clientTransport != null ? clientTransport : serverTransport))
        .withServerSelectionStrategy(new CombinedSelectionStrategy(clientAddress));

      CopycatClient client = clientBuilder.build();
      client.serializer().resolve(new ResourceManagerTypeResolver());
      return new ResourceClient(new CombinedCopycatClient(client, serverTransport));
    }

    /**
     * Builds a server for the replica.
     */
    private ResourceServer buildServer() {
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

      // If the quorum hint is ALL then set the local member to ACTIVE.
      if (quorumHint == Quorum.ALL.size()) {
        serverBuilder.withType(Member.Type.ACTIVE);
      }

      CopycatServer server = serverBuilder.build();
      server.serializer().resolve(new ResourceManagerTypeResolver());
      return new ResourceServer(server);
    }

    /**
     * Builds a balancer for the replica.
     */
    private ClusterBalancer buildBalancer() {
      return new ClusterBalancer(quorumHint == Quorum.SEED.size() ? members.size() : quorumHint, backupCount);
    }

    /**
     * Builds the replica.
     * <p>
     * If no {@link Transport} was configured for the replica, the builder will attempt to create a
     * {@code NettyTransport} instance. If {@code io.atomix.catalyst.transport.NettyTransport} is not available
     * on the classpath, a {@link ConfigurationException} will be thrown.
     * <p>
     * Once the replica is built, it is not yet connected to the cluster. To connect the replica to the cluster,
     * call the asynchronous {@link #open()} method.
     *
     * @return The built replica.
     * @throws ConfigurationException if the replica is misconfigured
     */
    @Override
    public AtomixReplica build() {
      return new AtomixReplica(buildClient(), buildServer(), buildBalancer());
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
    public CompletableFuture<CopycatClient> open() {
      return client.open();
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
    public boolean isOpen() {
      return client.isOpen();
    }

    @Override
    public boolean isClosed() {
      return client.isClosed();
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
