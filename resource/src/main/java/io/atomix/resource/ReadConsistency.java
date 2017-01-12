/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.resource;

import io.atomix.copycat.Query;

/**
 * Constants for controlling read consistency constraints on a per-request basis.
 * <p>
 * Read consistency levels dictate how queries should be evaluated in the Atomix cluster. Resources generally have
 * significantly greater flexibility in controlling the consistency level of reads because Raft allows reads from
 * leaders or followers. Consistency levels can be applied on a per-command basis or to all operations for a given
 * resource.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum ReadConsistency {

  /**
   * Guarantees atomicity (linearizability) for read operations.
   * <p>
   * The atomic consistency level guarantees linearizability by contacting a majority of the cluster on every read.
   * When a {@link Query} is submitted to the cluster with linearizable consistency, it must be
   * forwarded to the current cluster leader. Once received by the leader, the leader will contact a majority of the
   * cluster before applying the query to its state machine and returning the result. Note that if the leader is already
   * in the process of contacting a majority of the cluster, it will queue the {@link Query} to
   * be processed on the next round trip. This allows the leader to batch expensive quorum based reads for efficiency.
   */
  ATOMIC(Query.ConsistencyLevel.LINEARIZABLE),

  /**
   * Provides linearizability under a leader lease.
   * <p>
   * Atomic lease consistency is a special implementation of linearizable reads that relies on the semantics of Raft's
   * election timers to determine whether it is safe to immediately apply a query to the Raft state machine. When a
   * linearizable {@link Query} is submitted to the Raft cluster with linearizable consistency,
   * it must be forwarded to the current cluster leader. For lease-based linearizability, the leader will determine whether
   * it's safe to apply the query to its state machine based on the last time it successfully contacted a majority of the
   * cluster. If the leader contacted a majority of the cluster within the last election timeout, it assumes that no other
   * member could have since become the leader and immediately applies the query to its state machine. Alternatively, if it
   * hasn't contacted a majority of the cluster within an election timeout, the leader will handle the query as if it were
   * submitted with {@link #ATOMIC} consistency.
   */
  ATOMIC_LEASE(Query.ConsistencyLevel.LINEARIZABLE_LEASE),

  /**
   * Guarantees sequential consistency for read operations.
   * <p>
   * Sequential read consistency requires that clients always see state progress in monotonically increasing order. Note that
   * this constraint allows reads from followers. When a sequential {@link Query} is submitted to the cluster, the first
   * server that receives the query will handle it. However, in order to ensure that state does not go back in time, the
   * client must submit its last known index with the query as well. If the server that receives the query has not advanced
   * past the provided client index, it will queue the query and await more entries from the leader.
   */
  SEQUENTIAL(Query.ConsistencyLevel.SEQUENTIAL),

  /**
   * Reads state from a local cache if possible, otherwise guarantees sequential consistency.
   * <p>
   * This is a special consistency level specifically for use in resources that support local caching. When state is read
   * using {@code LOCAL} read consistency, cached resources will attempt to service the read from the local cache and
   * fall back to {@link #SEQUENTIAL} communication with the cluster. State changes in the local cache must be similarly
   * guaranteed to occur in sequential order.
   */
  LOCAL(Query.ConsistencyLevel.SEQUENTIAL);

  private final Query.ConsistencyLevel level;

  ReadConsistency(Query.ConsistencyLevel level) {
    this.level = level;
  }

  public Query.ConsistencyLevel level() {
    return level;
  }

}
