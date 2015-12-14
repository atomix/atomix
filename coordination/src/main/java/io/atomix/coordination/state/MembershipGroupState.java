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

import io.atomix.coordination.Message;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.SessionListener;
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
public class MembershipGroupState extends ResourceStateMachine implements SessionListener {
  private final Map<Long, Commit<MembershipGroupCommands.Join>> members = new HashMap<>();
  private final Map<Long, Map<String, Commit<MembershipGroupCommands.SetProperty>>> properties = new HashMap<>();

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
    Commit<MembershipGroupCommands.Join> commit = members.remove(session.id());
    if (commit != null) {
      commit.close();
    }

    Map<String, Commit<MembershipGroupCommands.SetProperty>> properties = this.properties.remove(session.id());
    if (properties != null) {
      properties.values().forEach(Commit::close);
    }

    for (Commit<MembershipGroupCommands.Join> member : members.values()) {
      member.session().publish("leave", session.id());
    }
  }

  /**
   * Applies join commits.
   */
  public Set<Long> join(Commit<MembershipGroupCommands.Join> commit) {
    try {
      Commit<MembershipGroupCommands.Join> previous = members.put(commit.session().id(), commit);
      if (previous != null) {
        previous.close();
      } else {
        for (Commit<MembershipGroupCommands.Join> member : members.values()) {
          if (member.index() != commit.index()) {
            member.session().publish("join", commit.session().id());
          }
        }
      }
      return new HashSet<>(members.keySet());
    } catch (Exception e) {
      commit.close();
      throw e;
    }
  }

  /**
   * Applies leave commits.
   */
  public void leave(Commit<MembershipGroupCommands.Leave> commit) {
    try {
      Commit<MembershipGroupCommands.Join> previous = members.remove(commit.session().id());
      if (previous != null) {
        previous.close();

        Map<String, Commit<MembershipGroupCommands.SetProperty>> properties = this.properties.remove(commit.session().id());
        if (properties != null) {
          properties.values().forEach(Commit::close);
        }

        for (Commit<MembershipGroupCommands.Join> member : members.values()) {
          member.session().publish("leave", commit.session().id());
        }
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a list commit.
   */
  public Set<Long> list(Commit<MembershipGroupCommands.List> commit) {
    try {
      return new HashSet<>(members.keySet());
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a set property commit.
   */
  public void setProperty(Commit<MembershipGroupCommands.SetProperty> commit) {
    Map<String, Commit<MembershipGroupCommands.SetProperty>> properties = this.properties.get(commit.session().id());
    if (properties == null) {
      properties = new HashMap<>();
      this.properties.put(commit.session().id(), properties);
    }
    properties.put(commit.operation().property(), commit);
  }

  /**
   * Handles a set property commit.
   */
  public Object getProperty(Commit<MembershipGroupCommands.GetProperty> commit) {
    try {
      Map<String, Commit<MembershipGroupCommands.SetProperty>> properties = this.properties.get(commit.operation().member());
      if (properties != null) {
        Commit<MembershipGroupCommands.SetProperty> value = properties.get(commit.operation().property());
        return value != null ? value.operation().value() : null;
      }
      return null;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a set property commit.
   */
  public void removeProperty(Commit<MembershipGroupCommands.RemoveProperty> commit) {
    try {
      Map<String, Commit<MembershipGroupCommands.SetProperty>> properties = this.properties.get(commit.session().id());
      if (properties != null) {
        Commit<MembershipGroupCommands.SetProperty> previous = properties.remove(commit.operation().property());
        if (previous != null) {
          previous.close();
        }

        if (properties.isEmpty()) {
          this.properties.remove(commit.session().id());
        }
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a send commit.
   */
  public void send(Commit<MembershipGroupCommands.Send> commit) {
    try {
      Commit<MembershipGroupCommands.Join> join = members.get(commit.operation().member());
      if (join == null) {
        throw new IllegalArgumentException("unknown member: " + commit.operation().member());
      }

      join.session().publish("message", new Message(commit.operation().topic(), commit.operation().message()));
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a schedule commit.
   */
  public void schedule(Commit<MembershipGroupCommands.Schedule> commit) {
    try {
      if (!members.containsKey(commit.operation().member())) {
        throw new IllegalArgumentException("unknown member: " + commit.operation().member());
      }

      executor.schedule(Duration.ofMillis(commit.operation().delay()), () -> {
        Commit<MembershipGroupCommands.Join> member = members.get(commit.operation().member());
        if (member != null) {
          member.session().publish("execute", commit.operation().callback());
        }
        commit.close();
      });
    } catch (Exception e){
      commit.close();
      throw e;
    }
  }

  /**
   * Handles an execute commit.
   */
  public void execute(Commit<MembershipGroupCommands.Execute> commit) {
    try {
      Commit<MembershipGroupCommands.Join> member = members.get(commit.operation().member());
      if (member == null) {
        throw new IllegalArgumentException("unknown member: " + commit.operation().member());
      }

      member.session().publish("execute", commit.operation().callback());
    } finally {
      commit.close();
    }
  }

  @Override
  public void delete() {
    members.values().forEach(Commit::close);
    members.clear();
  }

}
