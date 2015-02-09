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
package net.kuujo.copycat.cluster.internal;

import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ConcurrentHashMap;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.election.Election;
import net.kuujo.copycat.election.ElectionEvent;
import net.kuujo.copycat.election.ElectionResult;
import net.kuujo.copycat.raft.RaftContext;

/**
 * Coordinated cluster election handler.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class CoordinatedClusterElection implements Election, Observer {
  private final Cluster cluster;
  private RaftContext context;
  private final Map<EventListener<ElectionEvent>, Boolean> listeners = new ConcurrentHashMap<>();
  private ElectionEvent result;
  private boolean handled;

  CoordinatedClusterElection(Cluster cluster, RaftContext context) {
    this.cluster = cluster;
    this.context = context;
  }

  @Override
  public void update(Observable o, Object arg) {
    RaftContext context = (RaftContext) o;
    if (!handled) {
      String leader = context.getLeader();
      if (leader != null) {
        long term = context.getTerm();
        if (term > 0) {
          Member member = cluster.member(leader);
          if (member != null) {
            result = new ElectionEvent(ElectionEvent.Type.COMPLETE, term, member);
            handled = true;
            for (EventListener<ElectionEvent> listener : listeners.keySet()) {
              listener.accept(result);
            }
          } else if (result != null) {
            result = null;
          }
        } else if (result != null) {
          result = null;
        }
      } else if (result != null) {
        result = null;
      }
    }
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
  public Election addListener(EventListener<ElectionEvent> listener) {
    if (listeners.putIfAbsent(listener, Boolean.TRUE) == null) {
      if (result != null && handled) {
        listener.accept(result);
      }
    }
    return this;
  }

  @Override
  public Election removeListener(EventListener<ElectionEvent> listener) {
    listeners.remove(listener);
    return this;
  }

  /**
   * Opens the election.
   */
  void open() {
    context.addObserver(this);
  }

  /**
   * Closes the election.
   */
  void close() {
    context.deleteObserver(this);
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, status=%s]", getClass().getCanonicalName(), term(), status());
  }

}
