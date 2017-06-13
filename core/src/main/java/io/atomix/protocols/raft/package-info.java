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
 * Core interfaces for operating on replicated state machines in the Copycat cluster.
 * <p>
 * The interfaces in this package are shared by both clients and servers. They are the interfaces through which clients and servers communicate
 * state change and query information with one another.
 * <p>
 * Clients operate on Copycat replicated state machines by submitting state change {@link io.atomix.copycat.Operation operations} to the cluster.
 * Copycat supports separate operations - {@link io.atomix.copycat.Command} and {@link io.atomix.copycat.Query} - for submitting state change
 * and read-only operations respectively. Each operation maps to a method of a replicated state machine. The handling of operations is dependent
 * on a variety of factors, including the operation type and the operation's {@link io.atomix.copycat.Query.ConsistencyLevel consistency level}.
 * When an operation is submitted to the cluster, the operation will eventually be translated into a method call on the replicated state machine
 * and a response will be sent back to the client.
 * <h2>Commands</h2>
 * {@link io.atomix.copycat.Command Commands} are operations that modify the state of the replicated state machine. A command is a serializable
 * object that will be sent to the leader of the cluster and replicated to a persisted on a majority of the servers in the Copycat cluster before
 * being applied to the state machine. Once a command is committed (stored on a majority of servers), it's translated into a method call on the
 * state machine on each server. The return value of the state machine method on the <em>leader</em> is sent back to the client.
 * <h2>Queries</h2>
 * {@link io.atomix.copycat.Query Queries} are operations that read but do not modify the state of the replicated state machine. Because queries
 * do not effect the state of the system, servers do not have to replicate them to a majority of the cluster, and no disk I/O is necessary to
 * complete a query of the state machine's state. Like commands, queries translate to a method call on the replicated state machine, but only
 * the server to which the query is submitted applies the query to its state machine. Once a query is completed, the return value of the state
 * machine method called is sent back to the client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix.protocols.raft;
