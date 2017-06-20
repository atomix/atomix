/*
 * Copyright 2017-present Open Networking Laboratory
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

import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.metadata.RaftSessionMetadata;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Copycat metadata.
 */
public interface RaftMetadataClient {

  /**
   * Returns the current cluster leader.
   *
   * @return The current cluster leader.
   */
  MemberId leader();

  /**
   * Returns the set of known servers in the cluster.
   *
   * @return The set of known servers in the cluster.
   */
  Collection<MemberId> servers();

  /**
   * Returns a list of open sessions.
   *
   * @return A completable future to be completed with a list of open sessions.
   */
  CompletableFuture<Set<RaftSessionMetadata>> getSessions();

  /**
   * Returns a list of open sessions of the given type.
   *
   * @return A completable future to be completed with a list of open sessions of the given type.
   */
  CompletableFuture<Set<RaftSessionMetadata>> getSessions(String type);

}
