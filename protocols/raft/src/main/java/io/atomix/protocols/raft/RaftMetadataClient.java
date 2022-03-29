// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.session.SessionMetadata;

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
  CompletableFuture<Set<SessionMetadata>> getSessions();

  /**
   * Returns a list of open sessions of the given type.
   *
   * @param primitiveType the service type for which to return sessions
   * @return A completable future to be completed with a list of open sessions of the given type.
   */
  CompletableFuture<Set<SessionMetadata>> getSessions(PrimitiveType primitiveType);

  /**
   * Returns a list of open sessions for the given service.
   *
   * @param primitiveType the service type for which to return sessions
   * @param serviceName the service for which to return sessions
   * @return A completable future to be completed with a list of open sessions of the given type.
   */
  CompletableFuture<Set<SessionMetadata>> getSessions(PrimitiveType primitiveType, String serviceName);

}
