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

import io.atomix.Quorum;
import io.atomix.copycat.error.ConfigurationException;
import io.atomix.copycat.server.cluster.Cluster;
import io.atomix.copycat.server.cluster.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Handles balancing of Atomix clusters.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ClusterBalancer implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterBalancer.class);
  private final int quorumHint;
  private final int backupCount;
  private boolean closed;

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
    if (closed) {
      future.completeExceptionally(new IllegalStateException("balancer closed"));
      return future;
    }

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

    BiConsumer<Void, Throwable> completeFunction = (result, error) -> {
      if (error == null || error.getCause() instanceof ConfigurationException) {
        balance(cluster, future);
      } else {
        future.completeExceptionally(error);
      }
    };

    // If the number of available active members is less than the quorum hint, promote a passive or reserve member.
    if (quorumHint == Quorum.ALL.size() || availableActiveCount < quorumHint) {
      // If a passive member is available, promote it.
      if (availablePassiveCount > 0) {
        Member promote = passive.stream().filter(m -> m.status() == Member.Status.AVAILABLE).findFirst().get();
        LOGGER.info("Promoting {} to ACTIVE: not enough active members", promote.address());
        promote.promote(Member.Type.ACTIVE).whenComplete(completeFunction);
        return future;
      }
      // If a reserve member is available, promote it.
      else if (availableReserveCount > 0) {
        Member promote = reserve.stream().filter(m -> m.status() == Member.Status.AVAILABLE).findFirst().get();
        LOGGER.info("Promoting {} to ACTIVE: not enough active members", promote.address());
          promote.promote(Member.Type.ACTIVE).whenComplete(completeFunction);
        return future;
      }
    }

    // If the total number of active members is greater than the quorum hint, demote an active member.
    // Preferably, we want to demote a member that is unavailable.
    if (quorumHint != Quorum.ALL.size() && totalActiveCount > quorumHint) {
      // If the number of available passive members is less than the required number, demote an active
      // member to passive.
      if (availablePassiveCount < (quorumHint - 1) * backupCount) {
        Member demote = active.stream().filter(m -> m.status() == Member.Status.UNAVAILABLE).findFirst()
          .orElseGet(() -> active.stream().filter(m -> !m.equals(member)).findAny().get());
        LOGGER.info("Demoting {} to PASSIVE: too many active members", demote.address());
        demote.demote(Member.Type.PASSIVE).whenComplete(completeFunction);
        return future;
      }
      // Otherwise, demote an active member to reserve.
      else {
        Member demote = active.stream().filter(m -> m.status() == Member.Status.UNAVAILABLE).findAny()
          .orElseGet(() -> active.stream().filter(m -> !m.equals(member)).findAny().get());
        LOGGER.info("Demoting {} to RESERVE: too many active members", demote.address());
        demote.demote(Member.Type.RESERVE).whenComplete(completeFunction);
        return future;
      }
    }

    // If the number of available passive members is less than the required number of passive members,
    // promote a reserve member.
    if (quorumHint != Quorum.ALL.size() && availablePassiveCount < (quorumHint - 1) * backupCount) {
      // If any reserve members are available, promote to passive.
      if (availableReserveCount > 0) {
        Member promote = reserve.stream().filter(m -> m.status() == Member.Status.AVAILABLE).findFirst().get();
        LOGGER.info("Promoting {} to PASSIVE: not enough passive members", promote.address());
        promote.promote(Member.Type.PASSIVE).whenComplete(completeFunction);
        return future;
      }
    }

    // If the total number of passive members is greater than the required number of passive members,
    // demote a passive member. Preferably we demote an unavailable member.
    if (quorumHint != Quorum.ALL.size() && totalPassiveCount > (quorumHint - 1) * backupCount) {
      Member demote = passive.stream().filter(m -> m.status() == Member.Status.UNAVAILABLE).findAny()
        .orElseGet(() -> passive.stream().findAny().get());
      LOGGER.info("Demoting {} to RESERVE: too many passive members", demote.address());
      demote.demote(Member.Type.RESERVE).whenComplete(completeFunction);
      return future;
    }

    // If we've made it this far then the cluster is balanced.
    future.complete(null);
    return future;
  }

  /**
   * Replaces the local member in the cluster.
   *
   * @param cluster The cluster in which to replace the local member.
   * @return A completable future to be completed once the local member has been replaced.
   */
  public CompletableFuture<Void> replace(Cluster cluster) {
    LOGGER.debug("Balancing cluster...");
    return replace(cluster, new CompletableFuture<>());
  }

  /**
   * Replaces the local member in the cluster.
   */
  private CompletableFuture<Void> replace(Cluster cluster, CompletableFuture<Void> future) {
    if (closed) {
      future.completeExceptionally(new IllegalStateException("cluster balancer closed"));
      return future;
    }

    BiConsumer<Void, Throwable> completeFunction = (result, error) -> {
      if (error == null) {
        future.complete(null);
      } else if (error.getCause() instanceof ConfigurationException) {
        replace(cluster, future);
      } else {
        future.completeExceptionally(error);
      }
    };

    Function<Void, CompletableFuture<Void>> demoteFunction = v -> {
      long passiveCount = cluster.members().stream().filter(m -> m.type() == Member.Type.PASSIVE).count();
      if (passiveCount < (quorumHint - 1) * backupCount) {
        LOGGER.info("Demoting {} to PASSIVE", cluster.member().address());
        return cluster.member().demote(Member.Type.PASSIVE);
      } else {
        LOGGER.info("Demoting {} to RESERVE", cluster.member().address());
        return cluster.member().demote(Member.Type.RESERVE);
      }
    };

    // If the quorum hint is ALL, don't replace the replica.
    if (quorumHint == Quorum.ALL.size()) {
      return CompletableFuture.completedFuture(null);
    }

    // If the local member is active, replace it with a passive or reserve member.
    if (cluster.member().type() == Member.Type.ACTIVE) {
      // Get a list of passive members.
      Collection<Member> passive = cluster.members().stream()
        .filter(m -> m.type() == Member.Type.PASSIVE)
        .collect(Collectors.toList());

      // Get a list of reserve members.
      Collection<Member> reserve = cluster.members().stream()
        .filter(m -> m.type() == Member.Type.RESERVE)
        .collect(Collectors.toList());

      // Attempt to promote an available passive member.
      if (!passive.isEmpty()) {
        Optional<Member> optionalMember = passive.stream().filter(m -> m.status() == Member.Status.AVAILABLE).findFirst();
        if (optionalMember.isPresent()) {
          LOGGER.info("Promoting {} to ACTIVE: replacing {}", optionalMember.get().address(), cluster.member().address());
          optionalMember.get().promote(Member.Type.ACTIVE)
            .thenCompose(demoteFunction)
            .whenComplete(completeFunction);
          return future;
        }
      }

      // Attempt to promote an available reserve member.
      if (!reserve.isEmpty()) {
        Optional<Member> optionalMember = reserve.stream().filter(m -> m.status() == Member.Status.AVAILABLE).findFirst();
        if (optionalMember.isPresent()) {
          LOGGER.info("Promoting {} to ACTIVE: replacing {}", optionalMember.get().address(), cluster.member().address());
          optionalMember.get().promote(Member.Type.ACTIVE)
            .thenCompose(demoteFunction)
            .whenComplete(completeFunction);
          return future;
        }
      }

      // Promote an unavailable passive or reserve member.
      if (!passive.isEmpty()) {
        Member member = passive.iterator().next();
        LOGGER.info("Promoting {} to ACTIVE: replacing {}", member.address(), cluster.member().address());
        member.promote(Member.Type.ACTIVE)
          .thenCompose(demoteFunction)
          .whenComplete(completeFunction);
      } else if (!reserve.isEmpty()) {
        Member member = reserve.iterator().next();
        LOGGER.info("Promoting {} to ACTIVE: replacing {}", member.address(), cluster.member().address());
        member.promote(Member.Type.ACTIVE)
          .thenCompose(demoteFunction)
          .whenComplete(completeFunction);
      } else {
        future.complete(null);
      }
    } else if (cluster.member().type() == Member.Type.PASSIVE) {
      Collection<Member> reserve = cluster.members().stream()
        .filter(m -> m.type() == Member.Type.RESERVE)
        .collect(Collectors.toList());

      if (!reserve.isEmpty()) {
        Optional<Member> optionalMember = reserve.stream().filter(m -> m.status() == Member.Status.AVAILABLE).findFirst();
        if (optionalMember.isPresent()) {
          LOGGER.info("Promoting {} to PASSIVE: replacing {}", optionalMember.get().address(), cluster.member().address());
          optionalMember.get().promote(Member.Type.PASSIVE)
            .thenCompose(demoteFunction)
            .whenComplete(completeFunction);
        } else {
          Member member = reserve.iterator().next();
          LOGGER.info("Promoting {} to PASSIVE: replacing {}", member.address(), cluster.member().address());
          member.promote(Member.Type.PASSIVE)
            .thenCompose(demoteFunction)
            .whenComplete(completeFunction);
        }
      } else {
        future.complete(null);
      }
    } else {
      future.complete(null);
    }
    return future;
  }

  @Override
  public void close() {
    closed = true;
  }

}
