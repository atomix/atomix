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
 * limitations under the License
 */
package io.atomix.util;

import io.atomix.copycat.server.cluster.Cluster;
import io.atomix.copycat.server.cluster.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Handles balancing of Atomix clusters.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ClusterBalancer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterBalancer.class);
  private final int quorumHint;
  private final int backupCount;

  public ClusterBalancer(int quorumHint, int backupCount) {
    this.quorumHint = quorumHint;
    this.backupCount = backupCount;
  }

  /**
   * Balances the given cluster.
   *
   * @param cluster The cluster to rebalance.
   * @return A completable future to be completed once the cluster has been balanced.
   */
  public CompletableFuture<Void> balance(Cluster cluster) {
    LOGGER.info("Balancing cluster...");
    return balance(cluster, new CompletableFuture<>());
  }

  /**
   * Balances the cluster, recursively promoting/demoting members as necessary.
   */
  private CompletableFuture<Void> balance(Cluster cluster, CompletableFuture<Void> future) {
    Collection<Member> members = cluster.members();
    Member member = cluster.member();

    Collection<Member> active = members.stream().filter(m -> m.type() == Member.Type.ACTIVE).collect(Collectors.toList());
    Collection<Member> passive = members.stream().filter(m -> m.type() == Member.Type.PASSIVE).collect(Collectors.toList());
    Collection<Member> reserve = members.stream().filter(m -> m.type() == Member.Type.RESERVE).collect(Collectors.toList());

    int totalActiveCount = active.size();
    int totalPassiveCount = passive.size();

    long availableActiveCount = active.stream().filter(m -> m.status() == Member.Status.AVAILABLE).count();
    long availablePassiveCount = passive.stream().filter(m -> m.status() == Member.Status.AVAILABLE).count();
    long availableReserveCount = reserve.stream().filter(m -> m.status() == Member.Status.AVAILABLE).count();

    // If the number of available active members is less than the quorum hint, promote a passive or reserve member.
    if (availableActiveCount < quorumHint) {
      // If a passive member is available, promote it.
      if (availablePassiveCount > 0) {
        passive.stream().filter(m -> m.status() == Member.Status.AVAILABLE).findFirst().get()
          .promote(Member.Type.ACTIVE)
          .thenRun(() -> balance(cluster, future));
        return future;
      }
      // If a reserve member is available, promote it.
      else if (availableReserveCount > 0) {
        reserve.stream().filter(m -> m.status() == Member.Status.AVAILABLE).findFirst().get()
          .promote(Member.Type.ACTIVE)
          .thenRun(() -> balance(cluster, future));
        return future;
      }
    }

    // If the total number of active members is greater than the quorum hint, demote an active member.
    // Preferably, we want to demote a member that is unavailable.
    if (totalActiveCount > quorumHint) {
      // If the number of available passive members is less than the required number, demote an active
      // member to passive.
      if (availablePassiveCount < quorumHint * backupCount) {
        active.stream().filter(m -> m.status() == Member.Status.UNAVAILABLE).findFirst()
          .orElseGet(() -> active.stream().filter(m -> !m.equals(member)).findAny().get())
          .demote(Member.Type.PASSIVE)
          .thenRun(() -> balance(cluster, future));
        return future;
      }
      // Otherwise, demote an active member to reserve.
      else {
        active.stream().filter(m -> m.status() == Member.Status.UNAVAILABLE).findAny()
          .orElseGet(() -> active.stream().filter(m -> !m.equals(member)).findAny().get())
          .demote(Member.Type.RESERVE)
          .thenRun(() -> balance(cluster, future));
        return future;
      }
    }

    // If the number of available passive members is less than the required number of passive members,
    // promote a reserve member.
    if (availablePassiveCount < quorumHint * backupCount) {
      // If any reserve members are available, promote to passive.
      if (availableReserveCount > 0) {
        reserve.stream().filter(m -> m.status() == Member.Status.AVAILABLE).findFirst().get()
          .promote(Member.Type.PASSIVE)
          .thenRun(() -> balance(cluster, future));
        return future;
      }
    }

    // If the total number of passive members is greater than the required number of passive members,
    // demote a passive member. Preferably we demote an unavailable member.
    if (totalPassiveCount > quorumHint * backupCount) {
      passive.stream().filter(m -> m.status() == Member.Status.UNAVAILABLE).findAny()
        .orElseGet(() -> passive.stream().findAny().get())
        .demote(Member.Type.RESERVE)
        .thenRun(() -> balance(cluster, future));
      return future;
    }

    // If we've made it this far then the cluster is balanced.
    future.complete(null);
    return future;
  }

}
