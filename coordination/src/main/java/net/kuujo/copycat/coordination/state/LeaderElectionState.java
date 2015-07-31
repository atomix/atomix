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

import net.kuujo.copycat.io.storage.Compaction;
import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.Filter;
import net.kuujo.copycat.raft.server.StateMachine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Leader election state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderElectionState extends StateMachine {
  private long version;
  private Session leader;
  private final List<Commit<LeaderElectionCommands.Listen>> listeners = new ArrayList<>();

  @Override
  public void close(Session session) {
    if (leader != null && leader.equals(session)) {
      leader = null;
      if (!listeners.isEmpty()) {
        Commit<LeaderElectionCommands.Listen> leader = listeners.remove(0);
        this.leader = leader.session();
        this.version = leader.index();
        this.leader.publish(true);
      }
    }
  }

  @Override
  public CompletableFuture<Object> apply(Commit<? extends Operation> commit) {
    return super.apply(commit);
  }

  /**
   * Applies listen commits.
   */
  @Apply(LeaderElectionCommands.Listen.class)
  protected void applyListen(Commit<LeaderElectionCommands.Listen> commit) {
    if (leader == null) {
      leader = commit.session();
      version = commit.index();
      leader.publish(true);
    } else {
      listeners.add(commit);
    }
  }

  /**
   * Applies listen commits.
   */
  @Apply(LeaderElectionCommands.Unlisten.class)
  protected void applyUnlisten(Commit<LeaderElectionCommands.Listen> commit) {
    if (leader != null && leader.equals(commit.session())) {
      leader = null;
      if (!listeners.isEmpty()) {
        Commit<LeaderElectionCommands.Listen> leader = listeners.remove(0);
        this.leader = leader.session();
        this.version = leader.index();
        this.leader.publish(true);
      }
    }
  }

  /**
   * Filters listen commits.
   */
  @Filter(LeaderElectionCommands.Listen.class)
  protected boolean filterListen(Commit<LeaderElectionCommands.Listen> commit, Compaction compaction) {
    return commit.index() >= version;
  }

  /**
   * Filters unlisten commits.
   */
  @Filter(LeaderElectionCommands.Unlisten.class)
  protected boolean filterUnlisten(Commit<LeaderElectionCommands.Unlisten> commit, Compaction compaction) {
    return commit.index() >= version;
  }

}
