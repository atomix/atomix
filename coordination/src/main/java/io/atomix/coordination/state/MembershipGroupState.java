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
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.resource.ResourceStateMachine;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Group state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MembershipGroupState extends ResourceStateMachine {
  private final Map<Long, Commit<MembershipGroupCommands.Join>> members = new HashMap<>();

  @Override
  public void configure(StateMachineExecutor executor) {
    executor.register(MembershipGroupCommands.Join.class, this::join);
    executor.register(MembershipGroupCommands.Leave.class, this::leave);
    executor.register(MembershipGroupCommands.Schedule.class, this::schedule);
    executor.register(MembershipGroupCommands.Execute.class, this::execute);
  }

  @Override
  public void close(Session session) {
    members.remove(session.id());
    for (Commit<MembershipGroupCommands.Join> member : members.values()) {
      member.session().publish("leave", session.id());
    }
  }

  /**
   * Applies join commits.
   */
  protected Set<Long> join(Commit<MembershipGroupCommands.Join> commit) {
    try {
      Commit<MembershipGroupCommands.Join> previous = members.put(commit.session().id(), commit);
      if (previous != null) {
        previous.clean();
      } else {
        for (Commit<MembershipGroupCommands.Join> member : members.values()) {
          if (member.index() != commit.index()) {
            member.session().publish("join", commit.session().id());
          }
        }
      }
      return new HashSet<>(members.keySet());
    } catch (Exception e) {
      commit.clean();
      throw e;
    }
  }

  /**
   * Applies leave commits.
   */
  protected void leave(Commit<MembershipGroupCommands.Leave> commit) {
    try {
      Commit<MembershipGroupCommands.Join> previous = members.remove(commit.session().id());
      if (previous != null) {
        previous.clean();
        for (Commit<MembershipGroupCommands.Join> member : members.values()) {
          member.session().publish("leave", commit.session().id());
        }
      }
    } finally {
      commit.clean();
    }
  }

  /**
   * Handles a schedule commit.
   */
  protected void schedule(Commit<MembershipGroupCommands.Schedule> commit) {
    try {
      if (!members.containsKey(commit.operation().member())) {
        throw new IllegalArgumentException("unknown member: " + commit.operation().member());
      }

      executor().schedule(Duration.ofMillis(commit.operation().delay()), () -> {
        Commit<MembershipGroupCommands.Join> member = members.get(commit.operation().member());
        if (member != null) {
          member.session().publish("execute", commit.operation().callback());
        }
        commit.clean();
      });
    } catch (Exception e){
      commit.clean();
      throw e;
    }
  }

  /**
   * Handles an execute commit.
   */
  protected void execute(Commit<MembershipGroupCommands.Execute> commit) {
    try {
      Commit<MembershipGroupCommands.Join> member = members.get(commit.operation().member());
      if (member == null) {
        throw new IllegalArgumentException("unknown member: " + commit.operation().member());
      }

      member.session().publish("execute", commit.operation().callback());
    } finally {
      commit.clean();
    }
  }

  @Override
  public void delete() {
    members.values().forEach(Commit::clean);
    members.clear();
  }

}
