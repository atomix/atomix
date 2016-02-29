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

import io.atomix.catalyst.transport.Address;
import io.atomix.coordination.DistributedGroup;
import io.atomix.coordination.GroupMemberInfo;
import io.atomix.coordination.GroupTask;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.resource.ResourceStateMachine;
import io.atomix.resource.ResourceType;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Group state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class GroupState extends ResourceStateMachine implements SessionListener {
  private final Set<ServerSession> sessions = new HashSet<>();
  private final Map<String, Commit<GroupCommands.Join>> members = new HashMap<>();
  private final Map<String, Queue<Commit<GroupCommands.Submit>>> tasks = new HashMap<>();
  private final Map<String, Map<String, Commit<GroupCommands.SetProperty>>> properties = new HashMap<>();
  private final Map<String, Commit<GroupCommands.SetProperty>> groupProperties = new HashMap<>();
  private final Queue<Commit<GroupCommands.Join>> candidates = new ArrayDeque<>();
  private Commit<GroupCommands.Join> leader;
  private long term;

  public GroupState() {
    super(new ResourceType(DistributedGroup.class));
  }

  @Override
  public void close(ServerSession session) {
    Map<Long, Commit<GroupCommands.Join>> left = new HashMap<>();

    // Remove the session from the sessions set.
    sessions.remove(session);

    // Iterate through all open members.
    Iterator<Map.Entry<String, Commit<GroupCommands.Join>>> iterator = members.entrySet().iterator();
    while (iterator.hasNext()) {
      // If the member is associated with the closed session, remove it from the members list.
      Commit<GroupCommands.Join> commit = iterator.next().getValue();
      if (commit.session().equals(session)) {
        iterator.remove();

        // Clear properties associated with the member.
        if (!commit.operation().persist()) {
          Map<String, Commit<GroupCommands.SetProperty>> properties = this.properties.remove(commit.operation().member());
          if (properties != null) {
            properties.values().forEach(Commit::close);
          }
        }

        // Remove the member's tasks from the task queue.
        Queue<Commit<GroupCommands.Submit>> tasks = this.tasks.remove(commit.operation().member());
        if (tasks != null) {
          // For each task, determine whether the task is complete and should be released.
          for (Commit<GroupCommands.Submit> task : tasks) {
            if (task.operation().member() == null) {
              // If the task was submitted to all members, determine if removing this member makes the
              // task complete.
              boolean complete = true;
              for (Queue<Commit<GroupCommands.Submit>> taskQueue : this.tasks.values()) {
                Commit<GroupCommands.Submit> firstTask = taskQueue.peek();
                if (firstTask.index() <= task.index()) {
                  complete = false;
                }
              }

              // If the task is complete, publish an acknowledgement back to the client.
              if (complete) {
                task.session().publish("ack", task.operation());
                task.close();
              }
            } else {
              // Publish an acknowledgement back to the client.
              task.session().publish("ack", task.operation());
              task.close();
            }
          }
        }

        candidates.remove(commit);
        left.put(commit.index(), commit);
      }
    }

    // If the current leader is one of the members that left the cluster, resign the leadership
    // and elect a new leader. This must be done after all the removed members are removed from internal state.
    if (leader != null && left.containsKey(leader.index())) {
      resignLeader(false);
      incrementTerm();
      electLeader();
    }

    // Iterate through the remaining sessions and publish a leave event for each removed member.
    sessions.forEach(s -> {
      if (s.state().active()) {
        for (Map.Entry<Long, Commit<GroupCommands.Join>> entry : left.entrySet()) {
          s.publish("leave", entry.getValue().operation().member());
        }
      }
    });

    // Close the commits for the members that left the group.
    for (Map.Entry<Long, Commit<GroupCommands.Join>> entry : left.entrySet()) {
      Commit<GroupCommands.Join> commit = entry.getValue();
      commit.close();
    }
  }

  /**
   * Increments the term.
   */
  private void incrementTerm() {
    term = context.index();
    for (ServerSession session : sessions) {
      if (session.state().active()) {
        session.publish("term", term);
      }
    }
  }

  /**
   * Resigns a leader.
   */
  private void resignLeader(boolean toCandidate) {
    if (leader != null) {
      for (ServerSession session : sessions) {
        if (session.state().active()) {
          session.publish("resign", leader.operation().member());
        }
      }

      if (toCandidate) {
        candidates.add(leader);
      }
      leader = null;
    }
  }

  /**
   * Elects a leader if necessary.
   */
  private void electLeader() {
    Commit<GroupCommands.Join> commit = candidates.poll();
    while (commit != null) {
      if (!commit.session().state().active()) {
        commit = candidates.poll();
      } else {
        leader = commit;
        for (ServerSession session : sessions) {
          if (session.state().active()) {
            session.publish("elect", leader.operation().member());
          }
        }
        break;
      }
    }
  }

  /**
   * Applies join commits.
   */
  public GroupMemberInfo join(Commit<GroupCommands.Join> commit) {
    try {
      String memberId = commit.operation().member();
      Address address = commit.operation().address();
      GroupMemberInfo info = new GroupMemberInfo(memberId, address);

      // Store the member ID and join commit mappings and add the member as a candidate.
      members.put(memberId, commit);
      candidates.add(commit);

      // Iterate through available sessions and publish a join event to each session.
      for (ServerSession session : sessions) {
        if (session.state().active()) {
          session.publish("join", info);
        }
      }

      // If the term has not yet been set, set it.
      if (term == 0) {
        incrementTerm();
      }

      // If a leader has not yet been elected, elect one.
      if (leader == null) {
        electLeader();
      }

      return info;
    } catch (Exception e) {
      commit.close();
      throw e;
    }
  }

  /**
   * Applies leave commits.
   */
  public void leave(Commit<GroupCommands.Leave> commit) {
    try {
      String memberId = commit.operation().member();

      // Remove the member from the members list.
      Commit<GroupCommands.Join> join = members.remove(memberId);
      if (join != null) {

        // Remove any properties set for the member.
        Map<String, Commit<GroupCommands.SetProperty>> properties = this.properties.remove(memberId);
        if (properties != null) {
          properties.values().forEach(Commit::close);
        }

        // Remove the member's tasks from the task queue.
        Queue<Commit<GroupCommands.Submit>> tasks = this.tasks.remove(memberId);
        if (tasks != null) {
          // For each task, determine whether the task is complete and should be released.
          for (Commit<GroupCommands.Submit> task : tasks) {
            if (task.operation().member() == null) {
              // If the task was submitted to all members, determine if removing this member makes the
              // task complete.
              boolean complete = true;
              for (Queue<Commit<GroupCommands.Submit>> taskQueue : this.tasks.values()) {
                Commit<GroupCommands.Submit> firstTask = taskQueue.peek();
                if (firstTask.index() <= task.index()) {
                  complete = false;
                }
              }

              // If the task is complete, publish an acknowledgement back to the client.
              if (complete) {
                task.session().publish("ack", task.operation());
                task.close();
              }
            } else {
              // Publish an acknowledgement back to the client.
              task.session().publish("ack", task.operation());
              task.close();
            }
          }
        }

        // Remove the join from the leader queue.
        candidates.remove(join);

        // If the leaving member was the leader, increment the term and elect a new leader.
        if (leader.operation().member().equals(memberId)) {
          resignLeader(false);
          incrementTerm();
          electLeader();
        }

        // Publish a leave event to all listening sessions.
        for (ServerSession session : sessions) {
          if (session.state().active()) {
            session.publish("leave", memberId);
          }
        }

        // Close the join commit to ensure it's garbage collected.
        join.close();
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a listen commit.
   */
  public Set<GroupMemberInfo> listen(Commit<GroupCommands.Listen> commit) {
    try {
      sessions.add(commit.session());
      Set<GroupMemberInfo> members = new HashSet<>();
      for (Commit<GroupCommands.Join> join : this.members.values()) {
        members.add(new GroupMemberInfo(join.operation().member(), join.operation().address()));
      }
      return members;
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a resign commit.
   */
  public void resign(Commit<GroupCommands.Resign> commit) {
    try {
      if (leader.operation().member().equals(commit.operation().member())) {
        resignLeader(true);
        incrementTerm();
        electLeader();
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a set property commit.
   */
  public void setProperty(Commit<GroupCommands.SetProperty> commit) {
    if (commit.operation().member() != null) {
      Map<String, Commit<GroupCommands.SetProperty>> properties = this.properties.get(commit.operation().member());
      if (properties == null) {
        properties = new HashMap<>();
        this.properties.put(commit.operation().member(), properties);
      }
      properties.put(commit.operation().property(), commit);
    } else {
      Commit<GroupCommands.SetProperty> oldCommit = groupProperties.put(commit.operation().property(), commit);
      if (oldCommit != null) {
        oldCommit.close();
      }
    }
  }

  /**
   * Handles a set property commit.
   */
  public Object getProperty(Commit<GroupCommands.GetProperty> commit) {
    try {
      if (commit.operation().member() != null) {
        Map<String, Commit<GroupCommands.SetProperty>> properties = this.properties.get(commit.operation().member());
        if (properties != null) {
          Commit<GroupCommands.SetProperty> value = properties.get(commit.operation().property());
          return value != null ? value.operation().value() : null;
        }
        return null;
      } else {
        Commit<GroupCommands.SetProperty> value = groupProperties.get(commit.operation().property());
        return value != null ? value.operation().value() : null;
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a set property commit.
   */
  public void removeProperty(Commit<GroupCommands.RemoveProperty> commit) {
    try {
      if (commit.operation().member() != null) {
        Map<String, Commit<GroupCommands.SetProperty>> properties = this.properties.get(commit.operation().member());
        if (properties != null) {
          Commit<GroupCommands.SetProperty> previous = properties.remove(commit.operation().property());
          if (previous != null) {
            previous.close();
          }

          if (properties.isEmpty()) {
            this.properties.remove(commit.operation().member());
          }
        }
      } else {
        Commit<GroupCommands.SetProperty> previous = groupProperties.remove(commit.operation().property());
        if (previous != null) {
          previous.close();
        }
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a submit commit.
   */
  public void submit(Commit<GroupCommands.Submit> commit) {
    try {
      if (commit.operation().member() != null) {
        // Ensure that the member is a member of the group.
        Commit<GroupCommands.Join> join = members.get(commit.operation().member());
        if (join == null) {
          throw new IllegalArgumentException("unknown member: " + commit.operation().member());
        }

        // Get the task queue for the member and add the task to the queue.
        Queue<Commit<GroupCommands.Submit>> tasks = this.tasks.computeIfAbsent(commit.operation().member(), m -> new ArrayDeque<>());
        tasks.add(commit);

        // If the added task is the only one in the queue, publish the task to the member.
        if (tasks.size() == 1) {
          join.session().publish("task", new GroupTask<>(commit.operation().id(), commit.operation().member(), commit.operation().task()));
        }
      } else {
        // Iterate through all the members in the group.
        for (Commit<GroupCommands.Join> member : members.values()) {

          // Get the task queue for the member and add the task to the queue.
          Queue<Commit<GroupCommands.Submit>> tasks = this.tasks.computeIfAbsent(commit.operation().member(), m -> new ArrayDeque<>());
          tasks.add(commit);

          // If the added task is the only one in the queue, publish the task to the member.
          if (tasks.size() == 1) {
            member.session().publish("task", new GroupTask<>(commit.operation().id(), member.operation().member(), commit.operation().task()));
          }
        }
      }
    } catch (Exception e) {
      commit.close();
      throw e;
    }
  }

  /**
   * Handles an ack commit.
   */
  public void ack(Commit<GroupCommands.Ack> commit) {
    try {
      // Get the task queue for the member.
      Queue<Commit<GroupCommands.Submit>> tasks = this.tasks.get(commit.operation().member());
      if (tasks != null) {

        // Ensure that the acknowledged task is the same as the next task in the queue.
        Commit<GroupCommands.Submit> task = tasks.peek();
        if (task.operation().id() == commit.operation().id()) {

          // If the acknowledged task matches, remove the task.
          tasks.remove();

          // If the original task was submitted to a specific member, send an ack event back to that member
          // and close the original task commit.
          if (task.operation().member() != null) {
            task.session().publish("ack", commit.operation());
            task.close();
          } else {
            // If the original task was submitted to all members, determine whether all enqueued tasks for the
            // source task have been completed based on the task index.
            boolean complete = true;
            for (Queue<Commit<GroupCommands.Submit>> taskQueue : this.tasks.values()) {
              Commit<GroupCommands.Submit> firstTask = taskQueue.peek();
              if (firstTask.index() <= task.index()) {
                complete = false;
              }
            }

            // If all members have completed their tasks, publish an ack event back to the client and close the
            // original task commit.
            if (complete) {
              task.session().publish("ack", commit.operation());
              task.close();
            }
          }
        }
      }
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a schedule commit.
   */
  public void schedule(Commit<GroupCommands.Schedule> commit) {
    try {
      Set<String> schedule = new HashSet<>();
      if (commit.operation().member() != null) {
        if (!members.containsKey(commit.operation().member())) {
          throw new IllegalArgumentException("unknown member: " + commit.operation().member());
        }
        schedule.add(commit.operation().member());
      } else {
        schedule.addAll(members.keySet());
      }

      AtomicInteger counter = new AtomicInteger();
      for (String memberId : schedule) {
        executor.schedule(Duration.ofMillis(commit.operation().delay()), () -> {
          Commit<GroupCommands.Join> member = members.get(memberId);
          if (member != null) {
            member.session().publish("execute", commit.operation().callback());
          }
          if (counter.incrementAndGet() == schedule.size()) {
            commit.close();
          }
        });
      }
    } catch (Exception e){
      commit.close();
      throw e;
    }
  }

  /**
   * Handles an execute commit.
   */
  public void execute(Commit<GroupCommands.Execute> commit) {
    try {
      if (commit.operation().member() != null) {
        Commit<GroupCommands.Join> member = members.get(commit.operation().member());
        if (member == null) {
          throw new IllegalArgumentException("unknown member: " + commit.operation().member());
        }
        member.session().publish("execute", commit.operation().callback());
      } else {
        for (Commit<GroupCommands.Join> member : members.values()) {
          member.session().publish("execute", commit.operation().callback());
        }
      }
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
