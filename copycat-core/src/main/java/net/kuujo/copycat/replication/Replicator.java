/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.replication;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.cluster.Member;

/**
 * Log replicator.<p>
 *
 * The replicator controls replication to a set of remote replicas.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Replicator {

  /**
   * Sets the required read quorum for replication.
   *
   * @param quorumSize The required number of replicas that must be up-to-date
   *        in order to perform reads.
   * @return The replicator instance.
   */
  Replicator withReadQuorum(Integer quorumSize);

  /**
   * Sets the required write quorum for replication.
   *
   * @param quorumSize The required number of replicas that must have received and
   *        logged an entry in order to consider it replicated. Note that replicated
   *        does not mean the same thing as committed.
   * @return The replicator instance.
   */
  Replicator withWriteQuorum(Integer quorumSize);

  /**
   * Adds a cluster member to the replica set.
   *
   * @param member The cluster member to add.
   * @return The replicator instance.
   */
  Replicator addMember(Member member);

  /**
   * Removes a cluster member from the replica set.
   *
   * @param member The cluster member to remove.
   * @return The replicator instance.
   */
  Replicator removeMember(Member member);

  /**
   * Returns a boolean indicating whether a cluster member is contained within
   * the replicator's replica set.
   *
   * @param member The cluster member to check.
   * @return Indicates whether the given member is a part of the replicator's replica set.
   */
  boolean containsMember(Member member);

  /**
   * Returns a set of cluster members in the replicator's replica set.
   *
   * @return A set of cluster members in the replicator's replica set.
   */
  Set<Member> getMembers();

  /**
   * Replicates all entries up to the given index to a quorum of the cluster.
   *
   * @param index The index up to which to replicate entries.
   * @return A completable future to be called once entries have been replicated
   *         to a write quorum of the cluster up to the given index.
   */
  CompletableFuture<Long> commit(long index);

  /**
   * Commits all log entries to a quorum of the cluster.
   *
   * @return A completable future to be called once all entries have been replicated
   *         to a write quorum of the cluster.
   */
  CompletableFuture<Long> commitAll();

  /**
   * Pings a read quorum of replicas to ensure they are all up-to-date according to
   * the given index.
   *
   * @param index The index up to which replicas should be synchronized.
   * @return A completable future to be completed once a read quorum of replicas have
   *         indicated that their logs are up-to-date according to the given index.
   */
  CompletableFuture<Long> ping(long index);

  /**
   * Pings a read quorum of replicas to ensure all log entries have been replicated.
   *
   * @return A completable future to be completed once a read quorum of replicas have
   *         indicated that they have received and logged all log entries.
   */
  CompletableFuture<Long> pingAll();

}
