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
package net.kuujo.copycat.election.impl;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.election.ElectionContext;
import net.kuujo.copycat.election.ElectionEvent;
import net.kuujo.copycat.election.ElectionListener;
import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.state.impl.RaftStateContext;
import net.kuujo.copycat.util.Quorum;

/**
 * Raft election context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftElectionContext implements ElectionContext {
  private final Set<ElectionListener> listeners = new HashSet<>();
  private final RaftStateContext state;
  private String currentLeader;
  private long currentTerm;

  public RaftElectionContext(RaftStateContext state) {
    this.state = state;
  }

  @Override
  public long currentTerm() {
    return currentTerm;
  }

  @Override
  public String currentLeader() {
    return currentLeader;
  }

  /**
   * Sets the election leader and term.
   */
  public void setLeaderAndTerm(long term, String leader) {
    if (leader != null && (currentLeader == null || !currentLeader.equals(leader))) {
      this.currentTerm = term;
      this.currentLeader = leader;
      triggerLeaderElected(term, leader);
    } else {
      this.currentTerm = term;
      this.currentLeader = leader;
    }
  }

  @Override
  public void addListener(ElectionListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(ElectionListener listener) {
    listeners.remove(listener);
  }

  /**
   * Triggers a leader elected event.
   */
  private void triggerLeaderElected(long term, String leader) {
    if (!listeners.isEmpty()) {
      ElectionEvent event = new ElectionEvent(term, leader);
      for (ElectionListener listener : listeners) {
        listener.leaderElected(event);
      }
    }
  }

  @Override
  public CompletableFuture<Boolean> start() {
    final CompletableFuture<Boolean> future = new CompletableFuture<>();
    final AtomicBoolean complete = new AtomicBoolean();

    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    final Quorum quorum = new Quorum(state.context().cluster().config().getQuorumSize(), (succeeded) -> {
      complete.set(true);
      future.complete(succeeded);
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    final long lastIndex = state.log().lastIndex();
    Entry lastEntry = state.log().getEntry(lastIndex);

    // Once we got the last log term, iterate through each current member
    // of the cluster and poll each member for a vote.
    final long lastTerm = lastEntry != null ? lastEntry.term() : 0;
    for (Member member : state.context().cluster().members()) {
      if (member.equals(state.context().cluster().localMember())) {
        quorum.succeed();
      } else {
        final ProtocolClient client = member.protocol().client();
        client.connect().whenCompleteAsync((result1, error1) -> {
          if (error1 != null) {
            quorum.fail();
          } else {
            client.requestVote(new RequestVoteRequest(state.nextCorrelationId(), state.getCurrentTerm(), state.context().cluster().config().getLocalMember(), lastIndex, lastTerm)).whenCompleteAsync((result2, error2) -> {
              client.close();
              if (!complete.get()) {
                if (error2 != null || !result2.voteGranted()) {
                  quorum.fail();
                } else {
                  quorum.succeed();
                }
              }
            });
          }
        });
      }
    }
    return future;
  }

}
