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

import net.kuujo.copycat.cluster.Member;

/**
 * Reference to a remote replica.<p>
 *
 * The replica handles replication of the log to a single remote replica.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Replica {

  /**
   * Returns the cluster member associated with this replica.
   *
   * @return The cluster member associated with the replica.
   */
  Member member();

  /**
   * Pings the replica up to the given log index. If necessary, log entries will
   * be sent to the replica to get it up to date.
   *
   * @param index The index up to which to ping the replica.
   * @return A completable future to be completed once the replica has responded.
   */
  CompletableFuture<Long> ping(long index);

  /**
   * Replicates entries to the replica up to the given log index. Note that since entries
   * are replicated in batches, additional entries may be replicated as well.
   *
   * @param index The index up to which to replicate entries.
   * @return A completable future to be completed once entries up to the given index
   *         have been successfully replicated to the replica.
   */
  CompletableFuture<Long> commit(long index);

}
