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
package net.kuujo.copycat.coordination.state;

import net.kuujo.copycat.raft.session.Session;
import net.kuujo.copycat.raft.Commit;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.StateMachineExecutor;

import java.util.ArrayList;
import java.util.List;

/**
 * Leader election state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderElectionState extends StateMachine {
  private Session leader;
  private final List<Commit<LeaderElectionCommands.Listen>> listeners = new ArrayList<>();

  @Override
  public void configure(StateMachineExecutor executor) {
    executor.register(LeaderElectionCommands.Listen.class, this::listen);
    executor.register(LeaderElectionCommands.Unlisten.class, this::unlisten);
  }

  @Override
  public void close(Session session) {
    if (leader != null && leader.equals(session)) {
      leader = null;
      if (!listeners.isEmpty()) {
        Commit<LeaderElectionCommands.Listen> leader = listeners.remove(0);
        this.leader = leader.session();
        this.leader.publish(true);
      }
    }
  }

  /**
   * Applies listen commits.
   */
  protected void listen(Commit<LeaderElectionCommands.Listen> commit) {
    if (leader == null) {
      leader = commit.session();
      leader.publish(true);
      commit.clean();
    } else {
      listeners.add(commit);
    }
  }

  /**
   * Applies listen commits.
   */
  protected void unlisten(Commit<LeaderElectionCommands.Unlisten> commit) {
    if (leader != null && leader.equals(commit.session())) {
      leader = null;
      if (!listeners.isEmpty()) {
        Commit<LeaderElectionCommands.Listen> leader = listeners.remove(0);
        this.leader = leader.session();
        this.leader.publish(true);
        leader.clean();
      }
    } else {
      commit.clean();
    }
  }

}
