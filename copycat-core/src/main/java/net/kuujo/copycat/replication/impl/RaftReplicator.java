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
package net.kuujo.copycat.replication.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.CopyCatException;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.replication.Replicator;
import net.kuujo.copycat.state.impl.RaftStateContext;
import net.kuujo.copycat.util.Quorum;

/**
 * Raft-based replicator implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftReplicator implements Replicator {
  private final RaftStateContext state;
  private final Map<String, RaftReplica> replicaMap = new HashMap<>();
  private final List<RaftReplica> replicas = new ArrayList<>();
  private int quorumSize;
  private int quorumIndex;
  private Integer readQuorum;
  private Integer writeQuorum;

  public RaftReplicator(RaftStateContext state) {
    this.state = state;
  }

  @Override
  public Replicator withReadQuorum(Integer quorumSize) {
    this.readQuorum = quorumSize;
    return this;
  }

  /**
   * Returns the read quorum size.
   */
  private int readQuorumSize() {
    return readQuorum != null ? readQuorum : quorumSize;
  }

  @Override
  public Replicator withWriteQuorum(Integer quorumSize) {
    this.writeQuorum = quorumSize;
    return this;
  }

  /**
   * Returns the write quorum size.
   */
  private int writeQuorumSize() {
    return writeQuorum != null ? writeQuorum : quorumSize;
  }

  /**
   * Recalculates quorum sizes.
   */
  private void resetQuorumSize() {
    quorumSize = (int) Math.floor((replicaMap.size() + 1) / 2 + 1);
    quorumIndex = quorumSize > 0 ? quorumSize - 1 : 0;
  }

  @Override
  public synchronized Replicator addMember(Member member) {
    if (!replicaMap.containsKey(member.uri())) {
      RaftReplica replica = new RaftReplica(member, state);
      replicaMap.put(member.uri(), replica);
      replicas.add(replica);
      replica.open();
      resetQuorumSize();
    }
    return this;
  }

  @Override
  public synchronized Replicator removeMember(Member member) {
    RaftReplica replica = replicaMap.remove(member.uri());
    if (replica != null) {
      replica.close();
      Iterator<RaftReplica> iterator = replicas.iterator();
      while (iterator.hasNext()) {
        if (iterator.next().member().uri().equals(member.uri())) {
          iterator.remove();
        }
      }
      resetQuorumSize();
    }
    return this;
  }

  @Override
  public synchronized boolean containsMember(Member member) {
    return replicaMap.containsKey(member.uri());
  }

  @Override
  public synchronized Set<Member> getMembers() {
    Set<Member> members = new HashSet<>();
    for (RaftReplica replica : replicaMap.values()) {
      members.add(replica.member());
    }
    return members;
  }

  @Override
  public CompletableFuture<Long> commit(long index) {
    CompletableFuture<Long> future = new CompletableFuture<>();

    // Set up a write quorum. Once the log entry has been replicated to
    // the required number of replicas in order to meet the write quorum
    // requirement, the future will succeed.
    final Quorum quorum = new Quorum(writeQuorumSize(), succeeded -> {
      if (succeeded) {
        future.complete(index);
      } else {
        future.completeExceptionally(new CopyCatException("Failed to obtain quorum"));
      }
    });

    // Iterate through replicas and commit all entries up to the given index.
    for (RaftReplica replica : replicaMap.values()) {
      replica.commit(index).whenComplete((resultIndex, error) -> {
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
  public CompletableFuture<Long> commitAll() {
    return commit(state.log().lastIndex());
  }

  @Override
  public CompletableFuture<Long> ping(long index) {
    CompletableFuture<Long> future = new CompletableFuture<>();

    // Set up a read quorum. Once the required number of replicas have been
    // contacted the quorum will succeed.
    final Quorum quorum = new Quorum(readQuorumSize(), succeeded -> {
      if (succeeded) {
        future.complete(index);
      } else {
        future.completeExceptionally(new CopyCatException("Failed to obtain quorum"));
      }
    });

    // Iterate through replicas and ping each replica. Internally, this
    // should cause the replica to send any remaining entries if necessary.
    for (RaftReplica replica : replicaMap.values()) {
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

  /**
   * Determines which message have been committed.
   */
  private void checkCommits() {
    if (!replicas.isEmpty() && quorumIndex > 0) {
      // Sort the list of replicas, order by the last index that was replicated
      // to the replica. This will allow us to determine the median index
      // for all known replicated entries across all cluster members.
      Collections.sort(replicas, new Comparator<RaftReplica>() {
        @Override
        public int compare(RaftReplica o1, RaftReplica o2) {
          return Long.compare(o1.matchIndex(), o2.matchIndex());
        }
      });
  
      // Set the current commit index as the median replicated index.
      // Since replicas is a list with zero based indexes, use the negation of
      // the required quorum size to get the index of the replica with the least
      // possible quorum replication. That replica's match index is the commit index.
      // Set the commit index. Once the commit index has been set we can run
      // all tasks up to the given commit.
      state.setCommitIndex(replicas.get(quorumIndex).matchIndex());
    }
  }

}
