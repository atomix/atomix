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
 * limitations under the License.
 */
package io.atomix.coordination.state;

import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.resource.ResourceStateMachine;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Leader election state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderElectionState extends ResourceStateMachine implements SessionListener {
  private Commit<LeaderElectionCommands.Listen> leader;
  private final Map<Long, Commit<LeaderElectionCommands.Listen>> listeners = new LinkedHashMap<>();

  @Override
  public void register(Session session) {

  }

  @Override
  public void unregister(Session session) {

  }

  @Override
  public void expire(Session session) {

  }

  @Override
  public void close(Session session) {
    if (leader != null && leader.session().equals(session)) {
      leader.close();
      leader = null;
      if (!listeners.isEmpty()) {
        Iterator<Map.Entry<Long, Commit<LeaderElectionCommands.Listen>>> iterator = listeners.entrySet().iterator();
        leader = iterator.next().getValue();
        iterator.remove();
        leader.session().publish("elect", leader.index());
      }
    } else {
      Commit<LeaderElectionCommands.Listen> listener = listeners.remove(session.id());
      if (listener != null) {
        listener.close();
      }
    }
  }

  /**
   * Applies listen commits.
   */
  public void listen(Commit<LeaderElectionCommands.Listen> commit) {
    if (leader == null) {
      leader = commit;
      leader.session().publish("elect", leader.index());
    } else if (!listeners.containsKey(commit.session().id())) {
      listeners.put(commit.session().id(), commit);
    } else {
      commit.close();
    }
  }

  /**
   * Applies listen commits.
   */
  public void unlisten(Commit<LeaderElectionCommands.Unlisten> commit) {
    try {
      if (leader != null && leader.session().equals(commit.session())) {
        leader.close();
        leader = null;
        if (!listeners.isEmpty()) {
          Iterator<Map.Entry<Long, Commit<LeaderElectionCommands.Listen>>> iterator = listeners.entrySet().iterator();
          leader = iterator.next().getValue();
          iterator.remove();
          leader.session().publish("elect", leader.index());
        }
      } else {
        Commit<LeaderElectionCommands.Listen> listener = listeners.remove(commit.session().id());
        if (listener != null) {
          listener.close();
        }
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Applies an isLeader query.
   */
  public boolean isLeader(Commit<LeaderElectionCommands.IsLeader> commit) {
    return leader != null && leader.session().equals(commit.session()) && leader.index() == commit.operation().epoch();
  }

  @Override
  public void delete() {
    if (leader != null) {
      leader.close();
    }

    listeners.values().forEach(Commit::close);
    listeners.clear();
  }

}
