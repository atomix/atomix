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
package net.kuujo.copycat.internal.replication;

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.internal.util.Quorum;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Cluster replicator implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterReplicator implements Replicator, Observer {
  private final StateContext state;
  private final Map<String, ClusterReplica> replicaMap;
  private final List<ClusterReplica> replicas;
  private Integer readQuorum;
  private Integer writeQuorum;
  private int quorumIndex;
  private final TreeMap<Long, CompletableFuture<Long>> commitFutures = new TreeMap<>();

  public ClusterReplicator(StateContext state) {
    this.state = state;
    this.replicaMap = new HashMap<>(state.cluster().members().size());
    this.replicas = new ArrayList<>(state.cluster().members().size());
    init();
  }

  /**
   * Initializes the replicator.
   */
  private void init() {
    state.cluster().addObserver(this);
    clusterChanged(state.cluster());
  }

  /**
   * Recalculates quorum sizes.
   */
  @SuppressWarnings("unchecked")
  private void recalculateQuorumSize() {
    readQuorum = state.config().getReadQuorumStrategy().calculateQuorumSize(new ClusterConfig(state.cluster()));
    writeQuorum = state.config().getWriteQuorumStrategy().calculateQuorumSize(new ClusterConfig(state.cluster()));
    int quorumSize = (int) Math.floor((replicas.size() + 1) / 2) + 1;
    quorumIndex = quorumSize > 0 ? quorumSize - 1 : 0;
  }

  @Override
  public void update(Observable o, Object arg) {
    clusterChanged((Cluster<?, ?>) o);
  }

  /**
   * Called when the replicator cluster configuration has changed.
   */
  private synchronized void clusterChanged(Cluster<?, ?> cluster) {
    cluster.remoteMembers().forEach(member -> {
      if (!replicaMap.containsKey(member.id())) {
        ClusterReplica replica = new ClusterReplica(member, state);
        replicaMap.put(member.id(), replica);
        replicas.add(replica);
        replica.open();
        recalculateQuorumSize();
      }
    });

    Iterator<ClusterReplica> iterator = replicas.iterator();
    while (iterator.hasNext()) {
      ClusterReplica replica = iterator.next();
      if (cluster.remoteMember(replica.member().id()) == null) {
        replica.close();
        iterator.remove();
        replicaMap.remove(replica.member().id());
      }
    }
  }

  @Override
  public CompletableFuture<Long> replicate(long index) {
    CompletableFuture<Long> future = new CompletableFuture<>();

    // Set up a write quorum. Once the log entry has been replicated to
    // the required number of replicas in order to meet the write quorum
    // requirement, the future will succeed.
    final Quorum quorum = new Quorum(writeQuorum, succeeded -> {
      if (succeeded) {
        future.complete(index);
      } else {
        future.completeExceptionally(new CopycatException("Failed to obtain quorum"));
      }
    });

    // Iterate through replicas and commit all entries up to the given index.
    for (ClusterReplica replica : replicaMap.values()) {
      replica.replicate(index).whenComplete((resultIndex, error) -> {
        // Once the commit succeeds, check the commit index of all replicas.
        if (error == null) {
          quorum.succeed();
          checkCommits();
        } else {
          quorum.fail();
        }
      });
    }
    return future;
  }

  @Override
  public CompletableFuture<Long> replicateAll() {
    return commit(state.log().lastIndex());
  }

  @Override
  public CompletableFuture<Long> ping(long index) {
    CompletableFuture<Long> future = new CompletableFuture<>();

    // Set up a read quorum. Once the required number of replicas have been
    // contacted the quorum will succeed.
    final Quorum quorum = new Quorum(readQuorum, succeeded -> {
      if (succeeded) {
        future.complete(index);
      } else {
        future.completeExceptionally(new CopycatException("Failed to obtain quorum"));
      }
    });

    // Iterate through replicas and ping each replica. Internally, this
    // should cause the replica to send any remaining entries if necessary.
    for (ClusterReplica replica : replicaMap.values()) {
      replica.ping(index).whenComplete((resultIndex, error) -> {
        if (error == null) {
          quorum.succeed();
        } else {
          quorum.fail();
        }
      });
    }
    return future;
  }

  @Override
  public CompletableFuture<Long> pingAll() {
    return ping(state.log().lastIndex());
  }

  @Override
  public CompletableFuture<Long> commit(long index) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    commitFutures.put(index, future);
    replicate(index);
    return future;
  }

  @Override
  public CompletableFuture<Long> commitAll() {
    return commit(state.log().lastIndex());
  }

  /**
   * Determines which message have been committed.
   */
  private void checkCommits() {
    if (!replicas.isEmpty() && quorumIndex > 0) {
      // Sort the list of replicas, order by the last index that was replicated
      // to the replica. This will allow us to determine the median index
      // for all known replicated entries across all cluster members.
      Collections.sort(replicas, (o1, o2) -> Long.compare(o1.matchIndex(), o2.matchIndex()));

      // Set the current commit index as the median replicated index.
      // Since replicas is a list with zero based indexes, use the negation of
      // the required quorum size to get the index of the replica with the least
      // possible quorum replication. That replica's match index is the commit index.
      // Set the commit index. Once the commit index has been set we can run
      // all tasks up to the given commit.
      long commitIndex = replicas.get(quorumIndex).matchIndex();
      state.commitIndex(commitIndex);
      triggerCommitFutures(commitIndex);
    }
  }

  /**
   * Triggers commit futures up to the given index.
   */
  private synchronized void triggerCommitFutures(long index) {
    Iterator<Map.Entry<Long, CompletableFuture<Long>>> iterator = commitFutures.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, CompletableFuture<Long>> entry = iterator.next();
      if (entry.getKey() <= index) {
        iterator.remove();
        entry.getValue().complete(entry.getKey());
      } else {
        break;
      }
    }
  }

  @Override
  public String toString() {
    return String.format("ClusterReplicator[cluster=%s]", replicas);
  }

}
