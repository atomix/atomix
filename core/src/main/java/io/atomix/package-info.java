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

/**
 * Core Atomix APIs for creating and operating on distributed objects.
 * <h3>The Atomix cluster</h3>
 * This package provides the primary mechanisms for creating and managing Atomix clusters. An Atomix cluster
 * consists of one or more {@link io.atomix.AtomixReplica replicas} and any number of {@link io.atomix.AtomixClient clients}.
 * To create a cluster, construct an {@link io.atomix.AtomixReplica AtomixReplica}.
 * <pre>
 *   {@code
 *   Address address = new Address("localhost", 5000);
 *   List<Address> members = Arrays.asList(
 *     new Address("localhost", 5000),
 *     new Address("localhost", 5001),
 *     new Address("localhost", 5002)
 *   );
 *
 *   AtomixReplica replica = AtomixReplica.builder(address, members)
 *     .withTransport(new NettyTransport())
 *     .withStorage(new Storage("logs"))
 *     .withQuorumHint(3)
 *     .withBackupCount(1)
 *     .build();
 *   replica.open();
 *   }
 * </pre>
 * Atomix is built on the <a href="http://raft.github.io">Raft consensus algorithm</a> and so state changes
 * in the Atomix cluster are quorum-based. This typically necessitates clusters of {@code 3} or {@code 5} nodes.
 * <p>
 * But Atomix is designed to be embedded and scale in clusters much larger than the typical {@code 3} or {@code 5}
 * node consensus based cluster. To do so, Atomix assigns a portion of the cluster to participate in the Raft
 * algorithm, and the remainder of the replicas participate in asynchronous replication or await failures as standby
 * nodes. A typical Atomix cluster may consist of {@code 3} active Raft-voting replicas, {@code 2} backup replicas,
 * and any number of reserve nodes. As replicas are added to or removed from the cluster, Atomix transparently
 * balances the cluster to ensure it maintains the desired number of active and backup replicas.
 * <p>
 * Atomix clusters are configured by setting the desired number of Raft participating nodes - known as the
 * <em>quorum hint</em> - and the desired number of backup replicas - the <em>backup count</em>. Given these two
 * parameters, Atomix clusters balance themselves to ensure that at least the desired number of replicas are
 * participants in the Raft consensus algorithm at any time.
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
 * Backup replicas receive state changes asynchronously <em>after</em> they're committed via the Raft participating
 * replicas. Further replicating state changes to backup replicas allows active replicas to be easily replaced in
 * the event of a failure. If an active member of the Raft cluster is partitioned or crashes, Atomix will attempt to
 * replace it with a backup replica if possible. Because the backup is largely caught up to the partitioned replica,
 * replacing the Raft replica is a quick and painless operation. This allows Atomix clusters to handle failures of
 * a majority of the cluster so long as a majority of the Raft replicas do not fail simultaneously.
 * <h3>Resources</h3>
 * Atomix {@link io.atomix.AtomixReplica replicas} manage and replicate state, and both
 * {@link io.atomix.AtomixReplica replicas} and {@link io.atomix.AtomixClient clients} provide the interface
 * to operate on that state. The {@link io.atomix.Atomix Atomix} interface provides the methods necessary
 * to manage creating and operating on {@link io.atomix.resource.Resource Resources}. A resource is a stateful,
 * fault-tolerant, distributed object that's manged by a replicated state machine. To create a resource, simply
 * call {@link io.atomix.Atomix#get(java.lang.String, io.atomix.resource.ResourceType)} or
 * use one of the {@code getResource} methods like {@link io.atomix.Atomix#getMap(java.lang.String)}.
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("group").get();
 *   }
 * </pre>
 * A {@link io.atomix.resource.Resource resource} is simply an interface to a replicated state machine. When a
 * resource is created, a state machine is created on each {@link io.atomix.manager.ResourceServer server} or
 * {@link io.atomix.AtomixReplica replica} in the cluster, and operations on the resource are submitted to the
 * cluster where they're logged, replicated, and applied to the resource's specific {@link io.atomix.resource.ResourceStateMachine}.
 * <h4>Clients vs Replicas</h4>
 * The sole difference between {@link io.atomix.AtomixClient clients} and {@link io.atomix.AtomixReplica replicas} is
 * the ability to store state and participate in replication. Clients operate on resource state purely remotely. When
 * a resource created by an {@link io.atomix.AtomixClient AtomixClient} is modified, operations are submitted remotely
 * to the cluster. The same is true of replicas; when operations are performed on a resource created via an
 * {@link io.atomix.AtomixReplica AtomixReplica}, state changes may be performed remotely, but the replica also participates
 * in the replication and commitment of the state changes. This allows servers to be effectively be embedded in code that
 * operates on resource state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix;
