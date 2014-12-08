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
import net.kuujo.copycat.cluster.ClusterContext;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.election.ElectionResult;

import java.util.Observable;
import java.util.Observer;
import java.util.function.Consumer;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ClusterElection implements Election, Observer {
  private final Cluster cluster;
  private ClusterContext context;
  private Consumer<ElectionResult> handler;
  private ElectionResult result;

  ClusterElection(Cluster cluster, ClusterContext context) {
    this.cluster = cluster;
    this.context = context;
  }

  @Override
  public void update(Observable o, Object arg) {
    if (result == null) {
      ClusterContext context = (ClusterContext) o;
      String leader = context.getLeader();
      if (leader != null) {
        long term = context.getTerm();
        if (term > 0) {
          Member member = cluster.member(leader);
          if (member != null) {
            result = new ClusterElectionResult(term, member);
            if (handler != null) {
              handler.accept(result);
            }
          }
        }
      }
    }
    this.context = (ClusterContext) o;
  }

  @Override
  public Status status() {
    return context.getStatus();
  }

  @Override
  public long term() {
    return context.getTerm();
  }

  @Override
  public ElectionResult result() {
    return result;
  }

  @Override
  public Election handler(Consumer<ElectionResult> handler) {
    this.handler = handler;
    return this;
  }

  /**
   * Cluster election result.
   */
  private static class ClusterElectionResult implements ElectionResult {
    private final long term;
    private final Member winner;

    private ClusterElectionResult(long term, Member winner) {
      this.term = term;
      this.winner = winner;
    }

    @Override
    public long term() {
      return term;
    }

    @Override
    public Member winner() {
      return winner;
    }
  }

}
