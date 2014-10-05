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

import java.util.concurrent.CompletableFuture;

/**
 * Log replicator.<p>
 *
 * The replicator controls replication to a set of remote replicas. All
 * replication logic is contained within a replicator implementation.
 * The leader simply specifies the log index to replicate, and the replicator
 * notifies the leader once the entry has been replicated to a quorum of the
 * cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Replicator {

  /**
   * Replicates all entries up to the given index to a quorum of the cluster.<p>
   *
   * This method's behavior depends on the write quorum strategy. Once the entry
   * at the given index has been successfully replicated to the number of replicas
   * defined by the write quorum strategy, the returned future will succeed.
   *
   * @param index The index up to which to replicate entries.
   * @return A completable future to be called once entries have been replicated
   *         to a write quorum of the cluster up to the given index.
   */
  CompletableFuture<Long> replicate(long index);

  /**
   * Replicates all log entries to a quorum of the cluster.
   *
   * @return A completable future to be called once all entries have been replicated
   *         to a write quorum of the cluster.
   */
  CompletableFuture<Long> replicateAll();

  /**
   * Commits all entries up to the given index to a majority of the cluster.
   *
   * @param index The index up to which to replicate entries.
   * @return A completable future to be called once entries have been replicated
   *         to and committed on a majority of the cluster up to the given index.
   */
  CompletableFuture<Long> commit(long index);

  /**
   * Commits all log entries to a majority of the cluster.
   *
   * @return A completable future to be called once all entries have been replicated
   *         to a and committed on a majority of the cluster.
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
