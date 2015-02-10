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
 * This package contains Copycat's core Raft implementation.
 *
 * Copycat's Raft implementation is designed as a standalone module with no dependencies on other components of the
 * Copycat architecture such as protocols and resources.
 *
 * The core of the Raft implementation is contained within the {@link net.kuujo.copycat.raft.RaftContext} class. The
 * {@code RaftContext} provides a single interface to Copycat's Raft implementation. Rather than communicating via
 * Copycat's {@link net.kuujo.copycat.cluster.Cluster} interfaces directly, {@code RaftContext} implements the
 * {@link net.kuujo.copycat.raft.protocol.RaftProtocol} interface through which RPCs are executed and callbacks are
 * registered to represent communication outbound from the Raft algorithm. This allows the Raft implementation to be
 * easily unit testable.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package net.kuujo.copycat.raft;
