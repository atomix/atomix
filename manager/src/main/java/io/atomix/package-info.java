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
 * Core Atomix APIs, including {@link io.atomix.manager.ResourceClient} and {@link io.atomix.manager.ResourceReplica}.
 * <p>
 * <h3>The Atomix cluster</h3>
 * This package provides the primary mechanisms for creating and managing Atomix clusters. An Atomix cluster
 * consists of one or more {@link io.atomix.manager.ResourceServer servers} or {@link io.atomix.manager.ResourceReplica replicas}
 * and any number of clients. To create a cluster, construct an {@link io.atomix.manager.ResourceServer AtomixServer} or
 * {@link io.atomix.manager.ResourceReplica AtomixReplica}.
 * <p>
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
 *     .build();
 *   replica.open();
 *   }
 * </pre>
 * Atomix is built on the <a href="http://raft.github.io">Raft consensus algorithm</a> and so state changes
 * in the Atomix cluster are quorum-based. Atomix clusters typically consist of an odd number of servers or
 * replicas - e.g. {@code 3} or {@code 5} - to acheive the greatest level of fault tolerance.
 * <p>
 * <b>Servers vs Replicas</b>
 * <p>
 * The sole difference between {@link io.atomix.manager.ResourceServer servers} and {@link io.atomix.manager.ResourceReplica replicas}
 * is the ability to operate on {@link io.atomix.resource.Resource resources}. Servers should be used when constructing
 * a standalone system with which only {@link io.atomix.manager.ResourceClient clients} can communicate, and replicas should be
 * used for embedding a server while simultaneously operating on that server's state.
 * <p>
 * <h3>Resources</h3>
 * <p>
 * Atomix {@link io.atomix.manager.ResourceServer servers} and {@link io.atomix.manager.ResourceReplica replicas} provide the ability
 * to manage and replicate state, and {@link io.atomix.manager.ResourceReplica replicas} and {@link io.atomix.manager.ResourceClient clients}
 * provide the interface to operate on that state. The {@link io.atomix.manager.ResourceClient Atomix} interface provides the
 * methods necessary to manage creating and operating on {@link io.atomix.resource.Resource Resources}.
 * A resource is a stateful, fault-tolerant, distributed object that's manged by a replicated state machine. To
 * create a resource, simply call {@link io.atomix.manager.ResourceClient#get(java.lang.String, io.atomix.resource.ResourceType)} or
 * {@link io.atomix.manager.ResourceClient#create(java.lang.String, io.atomix.resource.ResourceType)} on a
 * {@link io.atomix.manager.ResourceClient client} or {@link io.atomix.manager.ResourceReplica replica}.
 * <p>
 * <pre>
 *   {@code
 *   DistributedGroup group = atomix.getGroup("group").get();
 *   }
 * </pre>
 * A {@link io.atomix.resource.Resource resource} is simply an interface to a replicated state machine. When a
 * resource is created, a state machine is created on each {@link io.atomix.manager.ResourceServer server} or
 * {@link io.atomix.manager.ResourceReplica replica} in the cluster, and operations on the resource are submitted to the
 * cluster where they're logged, replicated, and applied to the resource's specific {@link io.atomix.resource.ResourceStateMachine}.
 * <p>
 * <b>Clients vs Replicas</b>
 * <p>
 * The sole difference between {@link io.atomix.manager.ResourceClient clients} and {@link io.atomix.manager.ResourceReplica replicas} is
 * the ability to store state and participate in replication. Clients operate on resource state purely remotely. When
 * a resource created by an {@link io.atomix.manager.ResourceClient AtomixClient} is modified, operations are submitted remotely
 * to the cluster. The same is true of replicas; when operations are performed on a resource created via an
 * {@link io.atomix.manager.ResourceReplica AtomixReplica}, state changes may be performed remotely, but the replica also participates
 * in the replication and commitment of the state changes. This allows servers to be effectively be embedded in code that
 * operates on resource state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix;
