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
package io.atomix.protocols.raft;

/**
 * Base interface for operations that read system state.
 * <p>
 * Queries are submitted by clients to a Raft server to read Raft cluster-wide state. In contrast to
 * {@link RaftCommand commands}, queries allow for more flexible {@link ConsistencyLevel consistency levels} that trade
 * consistency for performance.
 * <h2>Consistency levels</h2>
 * All queries must specify a {@link #consistency()} with which to execute the query. The provided consistency level
 * dictates how queries are submitted to the Raft cluster. When a query is submitted to the cluster, the query is
 * sent in a message to the server to which the client is currently connected. The server handles the query requests
 * based on the configured {@link RaftQuery.ConsistencyLevel}. For {@link ConsistencyLevel#SEQUENTIAL} consistency, followers
 * are allowed to execute queries with certain constraints for faster reads. For higher consistency levels like
 * {@link ConsistencyLevel#LINEARIZABLE} and {@link ConsistencyLevel#LINEARIZABLE_LEASE}, queries are forwarded to the
 * cluster leader. See the {@link RaftQuery.ConsistencyLevel} documentation for more info.
 * <p>
 * By default, all queries should use the strongest consistency level, {@link ConsistencyLevel#LINEARIZABLE}.
 * It is essential that users understand the trade-offs in the various consistency levels before using them.
 * <h2>Serialization</h2>
 * Queries must be serializable both by the client and by all servers in the cluster. By default, all operations use
 * Java serialization. However, default serialization in slow because it requires the full class name and does not allocate
 * memory efficiently. For this reason, it's recommended that commands implement {@link io.atomix.catalyst.serializer.CatalystSerializable}
 * or register a custom {@link io.atomix.catalyst.serializer.TypeSerializer} for better performance. Serializable types
 * can be registered on the associated client/server {@link io.atomix.catalyst.serializer.Serializer} instance.
 *
 * @param <T> query result type
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 * @see ConsistencyLevel
 */
public interface RaftQuery<T> extends RaftOperation<T> {

  /**
   * Constants for specifying Raft {@link RaftQuery} consistency levels.
   * <p>
   * This enum provides identifiers for configuring consistency levels for {@link RaftQuery queries}
   * submitted to a Raft cluster.
   * <p>
   * Consistency levels are used to dictate how queries are routed through the Raft cluster and the requirements for
   * completing read operations based on submitted queries. For expectations of specific consistency levels, see below.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  enum ConsistencyLevel {

    /**
     * Enforces sequential query consistency.
     * <p>
     * Sequential read consistency requires that clients always see state progress in monotonically increasing order. Note that
     * this constraint allows reads from followers. When a sequential {@link RaftQuery} is submitted to the cluster, the first
     * server that receives the query will handle it. However, in order to ensure that state does not go back in time, the
     * client must submit its last known index with the query as well. If the server that receives the query has not advanced
     * past the provided client index, it will queue the query and await more entries from the leader.
     */
    SEQUENTIAL,

    /**
     * Enforces linearizable query consistency based on leader lease.
     * <p>
     * Bounded linearizability is a special implementation of linearizable reads that relies on the semantics of Raft's
     * election timers to determine whether it is safe to immediately apply a query to the Raft state machine. When a
     * linearizable {@link RaftQuery} is submitted to the Raft cluster with linearizable consistency,
     * it must be forwarded to the current cluster leader. For lease-based linearizability, the leader will determine whether
     * it's safe to apply the query to its state machine based on the last time it successfully contacted a majority of the
     * cluster. If the leader contacted a majority of the cluster within the last election timeout, it assumes that no other
     * member could have since become the leader and immediately applies the query to its state machine. Alternatively, if it
     * hasn't contacted a majority of the cluster within an election timeout, the leader will handle the query as if it were
     * submitted with {@link #LINEARIZABLE} consistency.
     */
    LINEARIZABLE_LEASE,

    /**
     * Enforces linearizable query consistency.
     * <p>
     * The linearizable consistency level guarantees consistency by contacting a majority of the cluster on every read.
     * When a {@link RaftQuery} is submitted to the cluster with linearizable consistency, it must be
     * forwarded to the current cluster leader. Once received by the leader, the leader will contact a majority of the
     * cluster before applying the query to its state machine and returning the result. Note that if the leader is already
     * in the process of contacting a majority of the cluster, it will queue the {@link RaftQuery} to
     * be processed on the next round trip. This allows the leader to batch expensive quorum based reads for efficiency.
     */
    LINEARIZABLE

  }

  /**
   * Returns the query consistency level.
   * <p>
   * The consistency will dictate how the query is executed on the server state. Stronger consistency levels can guarantee
   * linearizability in all or most cases, while weaker consistency levels trade linearizability for more performant
   * reads from followers. Consult the {@link ConsistencyLevel} documentation for more information
   * on the different consistency levels.
   * <p>
   * By default, this method enforces strong consistency with the {@link ConsistencyLevel#LINEARIZABLE} consistency level.
   *
   * @return The query consistency level.
   */
  default ConsistencyLevel consistency() {
    return ConsistencyLevel.LINEARIZABLE;
  }

}
