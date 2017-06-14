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
 * Classes and interfaces that aid in storing and loading persistent cluster and server configurations.
 * <p>
 * The {@link io.atomix.copycat.server.storage.system.MetaStore} is the primary interface through which servers
 * store and load server {@link io.atomix.copycat.server.storage.system.Configuration configurations} on disk.
 * Configurations include the current cluster configuration as well as the server's last
 * {@link io.atomix.copycat.server.storage.system.MetaStore#loadTerm() term} and
 * {@link io.atomix.copycat.server.storage.system.MetaStore#loadVote() vote} which must be persisted according
 * to the Raft consensus algorithm.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix.protocols.raft.server.storage.system;
