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

import net.kuujo.copycat.election.ElectionContext;
import net.kuujo.copycat.election.ElectionEvent;
import net.kuujo.copycat.election.ElectionListener;

/**
 * Raft election context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftElectionContext implements ElectionContext {
  private final Set<ElectionListener> listeners = new HashSet<>();
  private String currentLeader;
  private long currentTerm;

  public RaftElectionContext() {
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

}
