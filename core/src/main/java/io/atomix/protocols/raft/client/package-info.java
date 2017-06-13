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
 * Fully-featured Copycat client for interacting with replicated state machines.
 * <p>
 * The client is a fully featured client that facilitates submitting {@link io.atomix.copycat.Command commands}
 * and {@link io.atomix.copycat.Query queries} to a Copycat cluster. Clients communicate with the cluster within
 * the context of a session. To create a client and connect to the cluster, use the {@link io.atomix.copycat.client.CopycatClient.Builder}.
 * <pre>
 *   {@code
 *   CopycatClient client = CopycatClient.builder(servers)
 *     .withTransport(new NettyTransport())
 *     .build();
 *   client.open().join();
 *   }
 * </pre>
 * Clients are designed to communicate with any server in the cluster in a transparent manner. See the
 * {@link io.atomix.copycat.client.CopycatClient} documentation for more information.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix.protocols.raft.client;
