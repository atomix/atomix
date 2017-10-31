/*
 * Copyright 2017-present Open Networking Foundation
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
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.RaftSessionMetadata;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Raft metadata.
 */
public interface RaftMetadataClient {

  /**
   * Returns the current cluster leader.
   *
   * @return The current cluster leader.
   */
  MemberId getLeader();

  /**
   * Returns the set of known members in the cluster.
   *
   * @return The set of known members in the cluster.
   */
  default Collection<MemberId> getServers() {
    return getMembers();
  }

  /**
   * Returns the set of known members in the cluster.
   *
   * @return The set of known members in the cluster.
   */
  Collection<MemberId> getMembers();

  /**
   * Returns a list of open sessions.
   *
   * @return A completable future to be completed with a list of open sessions.
   */
  CompletableFuture<Set<RaftSessionMetadata>> getSessions();

  /**
   * Returns a list of open sessions of the given type.
   *
   * @param serviceType the service type for which to return sessions
   * @return A completable future to be completed with a list of open sessions of the given type.
   */
  default CompletableFuture<Set<RaftSessionMetadata>> getSessions(String serviceType) {
    return getSessions(ServiceType.from(serviceType));
  }

  /**
   * Returns a list of open sessions of the given type.
   *
   * @param serviceType the service type for which to return sessions
   * @return A completable future to be completed with a list of open sessions of the given type.
   */
  CompletableFuture<Set<RaftSessionMetadata>> getSessions(ServiceType serviceType);

  /**
   * Returns a list of open sessions for the given service.
   *
   * @param serviceType the service type for which to return sessions
   * @param serviceName the service for which to return sessions
   * @return A completable future to be completed with a list of open sessions of the given type.
   */
  default CompletableFuture<Set<RaftSessionMetadata>> getSessions(String serviceType, String serviceName) {
    return getSessions(ServiceType.from(serviceType), serviceName);
  }

  /**
   * Returns a list of open sessions for the given service.
   *
   * @param serviceType the service type for which to return sessions
   * @param serviceName the service for which to return sessions
   * @return A completable future to be completed with a list of open sessions of the given type.
   */
  CompletableFuture<Set<RaftSessionMetadata>> getSessions(ServiceType serviceType, String serviceName);

}
