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
package io.atomix.copycat.coordination.state;

import io.atomix.catalog.client.session.Session;
import io.atomix.catalog.server.Commit;
import io.atomix.catalog.server.StateMachine;
import io.atomix.catalog.server.StateMachineExecutor;

import java.util.HashMap;
import java.util.Map;

/**
 * Group state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class GroupState extends StateMachine {
  private final Map<Long, Commit<GroupCommands.Join>> members = new HashMap<>();

  @Override
  public void configure(StateMachineExecutor executor) {
    executor.register(GroupCommands.Join.class, this::join);
    executor.register(GroupCommands.Leave.class, this::leave);
    executor.register(GroupCommands.Execute.class, this::publish);
  }

  @Override
  public void close(Session session) {
    members.remove(session.id());
    for (Commit<GroupCommands.Join> member : members.values()) {
      member.session().publish("leave", session.id());
    }
  }

  /**
   * Applies join commits.
   */
  protected void join(Commit<GroupCommands.Join> commit) {
    try {
      Commit<GroupCommands.Join> previous = members.put(commit.session().id(), commit);
      if (previous != null) {
        previous.clean();
      } else {
        for (Commit<GroupCommands.Join> member : members.values()) {
          if (member.index() != commit.index()) {
            member.session().publish("join", commit.session().id());
          }
        }
      }
    } catch (Exception e) {
      commit.clean();
      throw e;
    }
  }

  /**
   * Applies leave commits.
   */
  protected void leave(Commit<GroupCommands.Leave> commit) {
    try {
      Commit<GroupCommands.Join> previous = members.remove(commit.session().id());
      if (previous != null) {
        previous.clean();
        for (Commit<GroupCommands.Join> member : members.values()) {
          member.session().publish("leave", commit.session().id());
        }
      }
    } finally {
      commit.clean();
    }
  }

  /**
   * Handles a publish commit.
   */
  protected void publish(Commit<GroupCommands.Execute> commit) {
    try {
      for (Commit<GroupCommands.Join> member : members.values()) {
        member.session().publish("execute", commit.operation().callback());
      }
    } finally {
      commit.clean();
    }
  }

}
