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
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.election.ElectionResult;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Resource cluster election handler.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ResourceClusterElection implements Election {
  private final Cluster cluster;
  private final Election election;
  private final ExecutionContext executor;
  private final Map<Consumer<ElectionResult>, Consumer<ElectionResult>> listeners = new HashMap<>();

  ResourceClusterElection(Cluster cluster, Election election, ExecutionContext executor) {
    this.cluster = cluster;
    this.election = election;
    this.executor = executor;
  }

  @Override
  public Status status() {
    return election.status();
  }

  @Override
  public long term() {
    return election.term();
  }

  @Override
  public ElectionResult result() {
    return election.result() != null ? new ResourceElectionResult(election.result()) : null;
  }

  @Override
  public Election addListener(Consumer<ElectionResult> listener) {
    if (!listeners.containsKey(listener)) {
      Consumer<ElectionResult> wrapper = result -> executor.execute(() -> listener.accept(result));
      election.addListener(wrapper);
      listeners.put(listener, wrapper);
    }
    return this;
  }

  @Override
  public Election removeListener(Consumer<ElectionResult> listener) {
    Consumer<ElectionResult> wrapper = listeners.remove(listener);
    if (wrapper != null) {
      election.removeListener(wrapper);
    }
    return this;
  }

  /**
   * Resource election result.
   */
  private class ResourceElectionResult implements ElectionResult {
    private final ElectionResult result;
    private ResourceElectionResult(ElectionResult result) {
      this.result = result;
    }

    @Override
    public long term() {
      return result.term();
    }

    @Override
    public Member winner() {
      Member winner = result.winner();
      return winner != null ? cluster.member(winner.uri()) : null;
    }
  }

}
