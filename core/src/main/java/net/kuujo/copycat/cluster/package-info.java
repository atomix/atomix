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
 * Classes within the {@code net.kuujo.copycat.cluster} package provide an interface for interacting with other
 * members within the Copycat cluster. Ultimately, all resources within a Copycat instance communicate via cluster
 * interfaces.
 *
 * Most notably, the {@link net.kuujo.copycat.cluster.Cluster} provides an interface for Copycat's Raft implementation
 * to communicate with other nodes in the cluster. Additionally, users can access the {@link net.kuujo.copycat.cluster.Cluster}
 * for any resource to communicate with associated members directly. The Copycat {@link net.kuujo.copycat.cluster.Cluster}
 * and its contained {@link net.kuujo.copycat.cluster.Member} instances provide a message bus for sending direct messages
 * between nodes.
 *
 * At the core of each node within a Copycat cluster is the cluster coordinator. The coordinator controls connections
 * from the local node to remote nodes and maintains membership information for the entire cluster. Each resource
 * managed by the coordinator must communicate over some subset of the coordinator's cluster members. In other words,
 * if the coordinator knows about nodes {@code A}, {@code B}, and {@code C} then resources within the local Copycat
 * instance can only talk to those three members or some smaller subset of them.
 *
 * Each resource managed by the cluster coordinator contains a reference to its own view of the Copycat
 * {@link net.kuujo.copycat.cluster.Cluster}. Thus, the cluster membership for one resource may differ from that of
 * another.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package net.kuujo.copycat.cluster;
