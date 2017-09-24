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
package io.atomix.cluster;

import io.atomix.AtomixReplica;
import io.atomix.catalyst.annotations.Experimental;
import io.atomix.catalyst.util.Assert;
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
 * Cluster manager implementation that automatically balances the cluster.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Experimental
public class BalancingClusterManager implements ClusterManager {

  /**
   * Returns a new balancing cluster manager builder.
   *
   * @return A new balancing cluster manager builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(BalancingClusterManager.class);
  private final int quorumHint;
  private final int backupCount;
  private boolean closed;

  public BalancingClusterManager(int quorumHint, int backupCount) {
    this.quorumHint = quorumHint;
    this.backupCount = backupCount;
  }

  @Override
  public CompletableFuture<Void> start(Cluster cluster, AtomixReplica replica) {
    cluster.members().forEach(m -> {
      m.onTypeChange(t -> balance(cluster));
      m.onStatusChange(s -> balance(cluster));
    });

    cluster.onLeaderElection(l -> balance(cluster));

    cluster.onJoin(m -> {
      m.onTypeChange(t -> balance(cluster));
      m.onStatusChange(s -> balance(cluster));
      balance(cluster);
    });

    cluster.onLeave(m -> balance(cluster));
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Balances the given cluster.
   *
   * @param cluster The cluster to rebalance.
   * @return A completable future to be completed once the cluster has been balanced.
   */
  public CompletableFuture<Void> balance(Cluster cluster) {
    if (cluster.member().equals(cluster.leader())) {
      LOGGER.info("Balancing cluster...");
      return balance(cluster, new CompletableFuture<>());
    }
    return CompletableFuture.completedFuture(null);
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

  @Override
  public CompletableFuture<Void> stop(Cluster cluster, AtomixReplica replica) {
    LOGGER.debug("Balancing cluster...");
    return replace(cluster, new CompletableFuture<>()).whenComplete((result, error) -> closed = true);
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

  /**
   * Balancing cluster manager builder.
   */
  public static class Builder implements ClusterManager.Builder {
    private int quorumHint = Quorum.ALL.size();
    private int backupCount = 0;

    /**
     * Sets the cluster quorum hint.
     * <p>
     * The quorum hint specifies the optimal number of replicas to actively participate in the Raft
     * consensus algorithm. As long as there are at least {@code quorumHint} replicas in the cluster,
     * Atomix will automatically balance replicas to ensure that at least {@code quorumHint} replicas
     * are active participants in the Raft algorithm at any given time. Replicas can be added to or
     * removed from the cluster at will, and remaining replicas will be transparently promoted and demoted
     * as necessary to maintain the desired quorum size.
     * <p>
     * The size of the quorum is relevant both to performance and fault-tolerance. When resources are
     * created or deleted or resource state changes are submitted to the cluster, Atomix will synchronously
     * replicate changes to a majority of the cluster before they can be committed and update state. For
     * example, in a cluster where the {@code quorumHint} is {@code 3}, a
     * {@link io.atomix.collections.DistributedMap#put(Object, Object)} command must be sent to the leader
     * and then synchronously replicated to one other replica before it can be committed and applied to the
     * map state machine. This also means that a cluster with {@code quorumHint} equal to {@code 3} can tolerate
     * at most one failure.
     * <p>
     * Users should set the {@code quorumHint} to an odd number of replicas or use one of the {@link Quorum}
     * magic constants for the greatest level of fault tolerance. Typically, in write-heavy workloads, the most
     * performant configuration will be a {@code quorumHint} of {@code 3}. In read-heavy workloads, quorum hints
     * of {@code 3} or {@code 5} can be used depending on the size of the cluster and desired level of fault tolerance.
     * Additional active replicas may or may not improve read performance depending on usage and in particular
     * {@link io.atomix.resource.ReadConsistency read consistency} levels.
     *
     * @param quorumHint The quorum hint. This must be the same on all replicas in the cluster.
     * @return The replica builder.
     * @throws IllegalArgumentException if the quorum hint is less than {@code -1}
     */
    public Builder withQuorumHint(int quorumHint) {
      this.quorumHint = Assert.argNot(quorumHint, quorumHint < -1, "quorumHint must be positive or -1");
      return this;
    }

    /**
     * Sets the cluster quorum hint.
     * <p>
     * The quorum hint specifies the optimal number of replicas to actively participate in the Raft
     * consensus algorithm. As long as there are at least {@code quorumHint} replicas in the cluster,
     * Atomix will automatically balance replicas to ensure that at least {@code quorumHint} replicas
     * are active participants in the Raft algorithm at any given time. Replicas can be added to or
     * removed from the cluster at will, and remaining replicas will be transparently promoted and demoted
     * as necessary to maintain the desired quorum size.
     * <p>
     * By default, the configured quorum hint is {@link Quorum#ALL}.
     * <p>
     * The size of the quorum is relevant both to performance and fault-tolerance. When resources are
     * created or deleted or resource state changes are submitted to the cluster, Atomix will synchronously
     * replicate changes to a majority of the cluster before they can be committed and update state. For
     * example, in a cluster where the {@code quorumHint} is {@code 3}, a
     * {@link io.atomix.collections.DistributedMap#put(Object, Object)} command must be sent to the leader
     * and then synchronously replicated to one other replica before it can be committed and applied to the
     * map state machine. This also means that a cluster with {@code quorumHint} equal to {@code 3} can tolerate
     * at most one failure.
     * <p>
     * Users should set the {@code quorumHint} to an odd number of replicas or use one of the {@link Quorum}
     * magic constants for the greatest level of fault tolerance. Typically, in write-heavy workloads, the most
     * performant configuration will be a {@code quorumHint} of {@code 3}. In read-heavy workloads, quorum hints
     * of {@code 3} or {@code 5} can be used depending on the size of the cluster and desired level of fault tolerance.
     * Additional active replicas may or may not improve read performance depending on usage and in particular
     * {@link io.atomix.resource.ReadConsistency read consistency} levels.
     *
     * @param quorum The quorum hint. This must be the same on all replicas in the cluster.
     * @return The replica builder.
     * @throws NullPointerException if the quorum hint is null
     */
    public Builder withQuorumHint(Quorum quorum) {
      this.quorumHint = Assert.notNull(quorum, "quorum").size();
      return this;
    }

    /**
     * Sets the replica backup count.
     * <p>
     * The backup count specifies the maximum number of replicas per {@link #withQuorumHint(int) active} replica
     * to participate in asynchronous replication of state. Backup replicas allow quorum-member replicas to be
     * more quickly replaced in the event of a failure or an active replica leaving the cluster. Additionally,
     * backup replicas may service {@link io.atomix.resource.ReadConsistency#SEQUENTIAL SEQUENTIAL} and
     * {@link io.atomix.resource.ReadConsistency#LOCAL LOCAL} reads to allow read operations to be further
     * spread across the cluster.
     * <p>
     * The backup count is used to calculate the number of backup replicas per non-leader active member. The
     * number of actual backups is calculated by {@code (quorumHint - 1) * backupCount}. If the
     * {@code backupCount} is {@code 1} and the {@code quorumHint} is {@code 3}, the number of backup replicas
     * will be {@code 2}.
     * <p>
     * By default, the backup count is {@code 0}, indicating no backups should be maintained. However, it is
     * recommended that each cluster have at least a backup count of {@code 1} to ensure active replicas can
     * be quickly replaced in the event of a network partition or other failure. Quick replacement of active
     * member nodes improves fault tolerance in cases where a majority of the active members in the cluster are
     * not lost simultaneously.
     *
     * @param backupCount The number of backup replicas per active replica. This must be the same on all
     *                    replicas in the cluster.
     * @return The replica builder.
     * @throws IllegalArgumentException if the {@code backupCount} is negative
     */
    public Builder withBackupCount(int backupCount) {
      this.backupCount = Assert.argNot(backupCount, backupCount < 0, "backupCount must be positive");
      return this;
    }

    @Override
    public ClusterManager build() {
      return new BalancingClusterManager(quorumHint, backupCount);
    }
  }

}
