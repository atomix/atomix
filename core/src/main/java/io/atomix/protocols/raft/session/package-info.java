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

/**
 * Facilitates communication with the Copycat cluster within the context of a session.
 * <p>
 * Clients communicate with the Copycat cluster within the context of a session. Sessions are used to
 * achieve linearizable semantics and first-in-first-out ordering of client operations by coordinating
 * {@link io.atomix.protocols.raft.RaftCommand commands} and {@link io.atomix.protocols.raft.RaftQuery queries} submitted by
 * the client to the cluster. Additionally, sessions facilitate listening for event notifications from
 * the cluster. When state changes occur in the server-side replicated state machine, state machines
 * can publish messages notifying the client of events. The session aids in guaranteeing sequential
 * and linearizable consistency for server-to-client communication.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix.protocols.raft.session;
