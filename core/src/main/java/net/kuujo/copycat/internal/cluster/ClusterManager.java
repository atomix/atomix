/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.spi.Protocol;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Internal cluster manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ClusterManager {

  /**
   * Returns the cluster protocol.
   *
   * @return The cluster protocol.
   */
  Protocol protocol();

  /**
   * Returns the underlying cluster.
   *
   * @return The underlying cluster.
   */
  Cluster cluster();

  /**
   * Returns a set of all members in the cluster.
   *
   * @return A set of all members in the cluster.
   */
  Set<Member> members();

  /**
   * Returns a cluster member by URI.
   *
   * @param uri The cluster member URI.
   * @return The cluster member.
   */
  Member member(String uri);

  /**
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  LocalMember localMember();

  /**
   * Returns a set of all remote members in the cluster.
   *
   * @return A set of all remote members in the cluster.
   */
  Set<RemoteMember> remoteMembers();

  /**
   * Polls the members of the cluster.
   *
   * @return A completable future indicating whether the local node was elected.
   */
  CompletableFuture<Boolean> poll();

  /**
   * Pings all members, performing a consistency check.
   *
   * @param index The index for which to perform the consistency check.
   * @return A completable future to be completed once a quorum of nodes have been pinged.
   */
  CompletableFuture<Long> ping(long index);

  /**
   * Commits all log entries up to the given index.
   *
   * @param index The index up to which to commit entries.
   * @return A completable future to be completed once the entries have been committed.
   */
  CompletableFuture<Long> commit(long index);

}
