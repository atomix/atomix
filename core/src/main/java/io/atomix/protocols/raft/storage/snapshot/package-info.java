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
 * Classes and interfaces that aid in storing, loading, and installing on-disk state machine snapshots.
 * <p>
 * Snapshots are byte-level representations of the state machine's state taken periodically to allow entries
 * to be removed from Copycat's {@link io.atomix.copycat.server.storage.Log} during log compaction.
 * Copycat interacts with snapshots primarily through the {@link io.atomix.copycat.server.storage.snapshot.SnapshotStore}
 * which is responsible for storing and loading snapshots. Each snapshot is stored as a separate file
 * on disk, and old snapshots may or may not be retained depending on the {@link io.atomix.copycat.server.storage.Storage}
 * configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix.protocols.raft.storage.snapshot;
