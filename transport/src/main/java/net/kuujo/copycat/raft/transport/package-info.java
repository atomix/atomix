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
 * The Transport layer provides a low-level interface via which Copycat clients and servers communicate.
 * <p>
 * A {@link net.kuujo.copycat.raft.transport.Transport} is a simple provider for {@link net.kuujo.copycat.raft.transport.Client}
 * and {@link net.kuujo.copycat.raft.transport.Server} objects. The {@link net.kuujo.copycat.raft.transport.Client} and
 * {@link net.kuujo.copycat.raft.transport.Server} are the interfaces through which clients and servers communicate respectively.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package net.kuujo.copycat.raft.transport;
