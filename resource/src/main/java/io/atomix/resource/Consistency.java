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
package io.atomix.resource;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;

/**
 * Resource consistency constraints.
 * <p>
 * This enum provides identifiers for configurable {@link Resource} consistency levels. Each operation
 * on a resource is submitted to the cluster with a configurable consistency level. The consistency level specifies
 * how the cluster should handle the operation and places constraints on the level of consistency required to complete
 * the operation.
 * <p>
 * The consistency level of a resource can be configured via {@link Resource#with(Consistency)}.
 * <pre>
 *   {@code
 *   Object value = map.with(Consistency.ATOMIC).get("key").get();
 *   }
 * </pre>
 * In some cases, such as with leader elections, strong consistency is critical to the correctness of a resource.
 * In those cases, consistency levels may be explicitly overridden by specific {@link Resource} implementations.
 * In that sense, consistency level configurations serve as a default. Consult specific resource documentation for
 * consistency implementation details.
 * <p>
 * Because of the semantics of the underlying Raft implementation, consistency levels differ in their meaning with
 * respect to read and write operations. Consult the specific consistency documentation for the implications of each.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum Consistency {

  /**
   * Provides no guarantees regarding write atomicity or order.
   * <p>
   * This consistency level places no additional burden of coordination on write operations submitted to the
   * cluster. This means an operation on any resource that modifies that resource's state may be applied to
   * the resource's replicated state machine more than once and it arbitrary order.
   * <p>
   * Read-only operations submitted with this consistency level are only guaranteed to be completed within
   * a bounded time of the leader, but order for concurrent queries is not enforced.
   * <p>
   * Resource events triggered by operations submitted with this consistency level are similarly not guaranteed
   * to be received by the client exactly once or in any particular order.
   */
  NONE {
    @Override
    public Command.ConsistencyLevel writeConsistency() {
      return Command.ConsistencyLevel.NONE;
    }

    @Override
    public Query.ConsistencyLevel readConsistency() {
      return Query.ConsistencyLevel.CAUSAL;
    }
  },

  /**
   * Guarantees atomicity for write operations and ensures clients see state progress monotonically for
   * non-concurrent operations.
   * <p>
   * Write operations submitted with this consistency level are guaranteed to be linearizable, meaning they
   * will completed some time between their invocation and response. Additionally, non-concurrent and concurrent
   * writes from a single client are guaranteed to be applied to the resource state in the order specified by
   * the client (program order) and responses for concurrent operations from a single client are guaranteed to
   * be received in the order specified by the client.
   * <p>
   * Non-overlapping read operations from a single client are guaranteed to see state progress monotonically.
   * That is, once the client receives the result of a read operation, any later read operation submitted by
   * the same client is guaranteed to see the same or more recent state than that read operation.
   * <p>
   * Events triggered by operations submitted with this consistency level are guaranteed to be received by
   * clients sequentially, but events are not guaranteed to be completed before the triggering operation.
   * For instance, if a client submits an unlock operation under this consistency level, and the unlock operation
   * notifies other clients that a lock has been released, the submitting client may receive the unlock response
   * before those other clients have been notified.
   */
  PROCESS {
    @Override
    public Command.ConsistencyLevel writeConsistency() {
      return Command.ConsistencyLevel.SEQUENTIAL;
    }

    @Override
    public Query.ConsistencyLevel readConsistency() {
      return Query.ConsistencyLevel.CAUSAL;
    }
  },

  /**
   * Guarantees atomicity for write operations and ensures clients will see state progress monotonically
   * for all operations.
   * <p>
   * Write operations submitted with this consistency level are guaranteed to be linearizable, meaning they
   * will completed some time between their invocation and response. Additionally, non-concurrent and concurrent
   * writes from a single client are guaranteed to be applied to the resource state in the order specified by
   * the client (program order) and responses for concurrent operations from a single client are guaranteed to
   * be received in the order specified by the client.
   * <p>
   * All read operations - including concurrent read operations - from a single client submitted with this
   * consistency level are guaranteed to see state progress monotonically. That is, if the client submits two
   * read operations, the first second operation is guaranteed to be completed after the first operation, and
   * the second operation is guaranteed to see the same or newer state as the first operation, regardless of
   * whether the two were submitted concurrently.
   * <p>
   * Events triggered by operations submitted with this consistency level are guaranteed to be received by
   * clients sequentially, but events are not guaranteed to be completed before the triggering operation.
   * For instance, if a client submits an unlock operation under this consistency level, and the unlock operation
   * notifies other clients that a lock has been released, the submitting client may receive the unlock response
   * before those other clients have been notified.
   */
  SEQUENTIAL {
    @Override
    public Command.ConsistencyLevel writeConsistency() {
      return Command.ConsistencyLevel.SEQUENTIAL;
    }

    @Override
    public Query.ConsistencyLevel readConsistency() {
      return Query.ConsistencyLevel.SEQUENTIAL;
    }
  },

  /**
   * Guarantees atomicity for all read and write operations and events.
   * <p>
   * Write operations submitted with this consistency level are guaranteed to be linearizable, meaning they
   * will completed some time between their invocation and response. Additionally, non-concurrent and concurrent
   * writes from a single client are guaranteed to be applied to the resource state in the order specified by
   * the client (program order) and responses for concurrent operations from a single client are guaranteed to
   * be received in the order specified by the client.
   * <p>
   * All read operations submitted with this consistency level are also guaranteed to be linearizable. If the
   * client submits a read operation, it is guaranteed to see the result of any prior write operation from any
   * other node in the cluster.
   * <p>
   * Events triggered by operations submitted with this consistency level are guaranteed to be received by
   * clients in the order in which they occurred in the state machine and prior to the completion of the
   * triggering operation. For instance, if a client submits an unlock operation under this consistency leave,
   * and the unlock operation notifies other clients that a lock has been released, those other clients are
   * guaranteed to receive the unlock event notification prior to the client receiving the unlock response.
   */
  ATOMIC {
    @Override
    public Command.ConsistencyLevel writeConsistency() {
      return Command.ConsistencyLevel.LINEARIZABLE;
    }

    @Override
    public Query.ConsistencyLevel readConsistency() {
      return Query.ConsistencyLevel.BOUNDED_LINEARIZABLE;
    }
  };

  /**
   * Returns the Copycat write consistency level.
   *
   * @return The Copycat write consistency level.
   */
  public abstract Command.ConsistencyLevel writeConsistency();

  /**
   * Returns the Copycat read consistency level.
   *
   * @return The Copycat read consistency level.
   */
  public abstract Query.ConsistencyLevel readConsistency();

}
