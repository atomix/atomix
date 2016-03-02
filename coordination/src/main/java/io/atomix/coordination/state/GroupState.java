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
  private final Map<Long, GroupSession> sessions = new HashMap<>();
  private final Map<String, Member> members = new HashMap<>();
  private final Map<String, Property> properties = new HashMap<>();
  private final Queue<Member> candidates = new ArrayDeque<>();
  private Member leader;
  private long term;

  public GroupState() {
    super(new ResourceType(DistributedGroup.class));
  }

  @Override
  public void close(ServerSession session) {
    Map<Long, Member> left = new HashMap<>();

    // Remove the session from the sessions set.
    sessions.remove(session.id());

    // Iterate through all open members.
    Iterator<Map.Entry<String, Member>> iterator = members.entrySet().iterator();
    while (iterator.hasNext()) {
      // If the member is associated with the closed session, remove it from the members list.
      Member member = iterator.next().getValue();
      if (member.session().equals(session) && !member.persistent()) {
        iterator.remove();
        candidates.remove(member);
        left.put(member.index(), member);
      }
    }

    // If the current leader is one of the members that left the cluster, resign the leadership
    // and elect a new leader. This must be done after all the removed members are removed from internal state.
    if (leader != null && left.containsKey(leader.index())) {
      resignLeader(false);
      incrementTerm();
      electLeader();
    }

    // Close the commits for the members that left the group.
    // Iterate through the remaining sessions and publish a leave event for each removed member
    // *after* the members have been closed to ensure events are sent in the proper order.
    left.values().forEach(member -> {
      member.close();
      sessions.values().forEach(s -> s.leave(member));
    });
  }

  /**
   * Increments the term.
   */
  private void incrementTerm() {
    term = context.index();
    sessions.values().forEach(s -> s.term(term));
  }

  /**
   * Resigns a leader.
   */
  private void resignLeader(boolean toCandidate) {
    if (leader != null) {
      sessions.values().forEach(s -> s.resign(leader));

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
    Member member = candidates.poll();
    while (member != null) {
      if (!member.session().state().active()) {
        member = candidates.poll();
      } else {
        leader = member;
        sessions.values().forEach(s -> s.elect(leader));
        break;
      }
    }
  }

  /**
   * Applies join commits.
   */
  public GroupMemberInfo join(Commit<GroupCommands.Join> commit) {
    try {
      Member member = members.get(commit.operation().member());

      // If the member doesn't already exist, create it.
      if (member == null) {
        member = new Member(commit);

        // Store the member ID and join commit mappings and add the member as a candidate.
        members.put(member.id(), member);
        candidates.add(member);

        // Iterate through available sessions and publish a join event to each session.
        for (GroupSession session : sessions.values()) {
          session.join(member);
        }

        // If the term has not yet been set, set it.
        if (term == 0) {
          incrementTerm();
        }

        // If a leader has not yet been elected, elect one.
        if (leader == null) {
          electLeader();
        }
      }
      // If the member already exists and is a persistent member, update the member to point to the new session.
      else if (member.persistent()) {
        // Iterate through available sessions and publish a join event to each session.
        // This will result in client-side groups updating the member object according to locality.
        for (GroupSession session : sessions.values()) {
          session.join(member);
        }

        // If the member is the group leader, force it to resign and elect a new leader. This is necessary
        // in the event the member is being reopened on another node.
        if (leader != null && leader.equals(member)) {
          resignLeader(true);
          incrementTerm();
          electLeader();
        }

        // Update the member's session to the commit session the member may have been reopened via a new session.
        member.session = commit.session();

        // Close the join commit since there's already an earlier commit that opened the member.
        // We have to retain the original commit that created the persistent member to ensure properties
        // created after the initial commit are retained and can be properly applied on replay.
        commit.close();
      }
      // If the member is not persistent, we can't override it.
      else {
        throw new IllegalArgumentException("cannot recreate ephemeral member");
      }
      return member.info();
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
      // Remove the member from the members list.
      Member member = members.remove(commit.operation().member());
      if (member != null) {
        // Remove the member from the candidates list.
        candidates.remove(member);

        // If the leaving member was the leader, increment the term and elect a new leader.
        if (leader != null && leader.equals(member)) {
          resignLeader(false);
          incrementTerm();
          electLeader();
        }

        // Close the member to ensure it's garbage collected.
        member.close();

        // Publish a leave event to all sessions *after* closing the member to ensure events
        // are received by clients in the proper order.
        sessions.values().forEach(s -> s.leave(member));
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
      sessions.put(commit.session().id(), new GroupSession(commit.session()));
      Set<GroupMemberInfo> members = new HashSet<>();
      for (Member member : this.members.values()) {
        members.add(member.info());
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
      Member member = members.get(commit.operation().member());
      if (member != null && leader != null && leader.equals(member)) {
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
      Member member = members.get(commit.operation().member());
      if (member != null) {
        Property property = new Property(commit);
        Property previous = member.properties.put(property.name(), property);
        if (previous != null) {
          previous.close();
        }
      }
    } else {
      Property property = new Property(commit);
      Property previous = properties.put(property.name(), property);
      if (previous != null) {
        previous.close();
      }
    }
  }

  /**
   * Handles a set property commit.
   */
  public Object getProperty(Commit<GroupCommands.GetProperty> commit) {
    try {
      if (commit.operation().member() != null) {
        Member member = members.get(commit.operation().member());
        if (member != null) {
          Property property = member.properties.get(commit.operation().property());
          return property != null ? property.value() : null;
        }
        return null;
      } else {
        Property property = properties.get(commit.operation().property());
        return property != null ? property.value() : null;
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
        Member member = members.get(commit.operation().member());
        if (member != null) {
          Property property = member.properties.remove(commit.operation().property());
          if (property != null) {
            property.close();
          }
        }
      } else {
        Property property = properties.remove(commit.operation().property());
        if (property != null) {
          property.close();
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
        Member member = members.get(commit.operation().member());
        if (member == null) {
          throw new IllegalArgumentException("unknown member: " + commit.operation().member());
        }

        // Create a task instance.
        Task task = new Task(commit);

        // Add the task to the member's task queue.
        member.submit(task);
      } else {
        // Create a task instance.
        Task task = new Task(commit);

        // Iterate through all the members in the group.
        for (Member member : members.values()) {
          member.submit(task);
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
      Member member = members.get(commit.operation().member());
      if (member != null) {
        if (commit.operation().succeeded()) {
          member.ack(commit.operation().id());
        } else {
          member.fail(commit.operation().id());
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
          Member member = members.get(memberId);
          if (member != null) {
            member.execute(commit.operation().callback());
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
        Member member = members.get(commit.operation().member());
        if (member == null) {
          throw new IllegalArgumentException("unknown member: " + commit.operation().member());
        }
        member.execute(commit.operation().callback());
      } else {
        for (Member member : members.values()) {
          member.execute(commit.operation().callback());
        }
      }
    } finally {
      commit.close();
    }
  }

  @Override
  public void delete() {
    members.values().forEach(Member::close);
    members.clear();
    properties.values().forEach(Property::close);
    properties.clear();
  }

  /**
   * Group session.
   */
  private static class GroupSession {
    private final ServerSession session;

    private GroupSession(ServerSession session) {
      this.session = session;
    }

    /**
     * Returns the session ID.
     */
    public long id() {
      return session.id();
    }

    /**
     * Sends a join event to the session for the given member.
     */
    public void join(Member member) {
      if (session.state().active()) {
        session.publish("join", new GroupMemberInfo(member.id(), member.address()));
      }
    }

    /**
     * Sends a leave event to the session for the given member.
     */
    public void leave(Member member) {
      if (session.state().active()) {
        session.publish("leave", member.id());
      }
    }

    /**
     * Sends a term event to the session for the given member.
     */
    public void term(long term) {
      if (session.state().active()) {
        session.publish("term", term);
      }
    }

    /**
     * Sends an elect event to the session for the given member.
     */
    public void elect(Member member) {
      if (session.state().active()) {
        session.publish("elect", member.id());
      }
    }

    /**
     * Sends a resign event to the session for the given member.
     */
    public void resign(Member member) {
      if (session.state().active()) {
        session.publish("resign", member.id());
      }
    }

    @Override
    public int hashCode() {
      return session.hashCode();
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof GroupSession && ((GroupSession) object).session.equals(session);
    }
  }

  /**
   * Represents a member of the group.
   */
  private static class Member implements AutoCloseable {
    private final Commit<GroupCommands.Join> commit;
    private final long index;
    private final String memberId;
    private final Address address;
    private final boolean persistent;
    private ServerSession session;
    private final Queue<Task> tasks = new ArrayDeque<>();
    private Task task;
    private final Map<String, Property> properties = new HashMap<>();

    private Member(Commit<GroupCommands.Join> commit) {
      this.commit = commit;
      this.index = commit.index();
      this.memberId = commit.operation().member();
      this.address = commit.operation().address();
      this.persistent = commit.operation().persist();
      this.session = commit.session();
    }

    /**
     * Returns the member index.
     */
    public long index() {
      return index;
    }

    /**
     * Returns the member ID.
     */
    public String id() {
      return memberId;
    }

    /**
     * Returns the member address.
     */
    public Address address() {
      return address;
    }

    /**
     * Returns group member info.
     */
    public GroupMemberInfo info() {
      return new GroupMemberInfo(memberId, address);
    }

    /**
     * Returns the member session.
     */
    public ServerSession session() {
      return session;
    }

    /**
     * Returns a boolean indicating whether the member is persistent.
     */
    public boolean persistent() {
      return persistent;
    }

    /**
     * Executes a callback on the member.
     */
    public void execute(Runnable callback) {
      if (session().state().active()) {
        session().publish("execute", callback);
      }
    }

    /**
     * Submits the given task to be processed by the member.
     */
    public void submit(Task task) {
      if (this.task == null) {
        this.task = task;
        if (session().state().active()) {
          session().publish("task", new GroupTask<>(task.index(), memberId, task.task()));
        }
      } else {
        tasks.add(task);
      }
    }

    /**
     * Acknowledges processing of a task.
     */
    public void ack(long id) {
      if (this.task.index() == id) {
        Task task = this.task;
        this.task = null;
        ack(task);
        next();
      }
    }

    /**
     * Acks the given task.
     */
    private void ack(Task task) {
      if (task.complete()) {
        task.ack();
        task.close();
      }
    }

    /**
     * Fails processing of a task.
     */
    public void fail(long id) {
      if (this.task.index() == id) {
        Task task = this.task;
        this.task = null;
        fail(task);
        next();
      }
    }

    /**
     * Fails the given task.
     */
    private void fail(Task task) {
      if (task.direct()) {
        task.fail();
        task.close();
      } else if (task.complete()) {
        task.ack();
        task.close();
      }
    }

    /**
     * Sends the next task in the queue.
     */
    private void next() {
      task = tasks.poll();
      if (task != null) {
        if (session().state().active()) {
          session().publish("task", new GroupTask<>(task.index(), memberId, task.task()));
        }
      }
    }

    @Override
    public void close() {
      // Remove any properties set for the member.
      properties.values().forEach(Property::close);
      properties.clear();

      Task task = this.task;
      this.task = null;
      if (task != null) {
        fail(task);
      }

      tasks.forEach(t -> {
        fail(task);
      });
      tasks.clear();

      commit.close();
    }

    @Override
    public int hashCode() {
      return commit.hashCode();
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Member && ((Member) object).id().equals(id());
    }
  }

  /**
   * Represents a group property.
   */
  private static class Property implements AutoCloseable {
    private final Commit<GroupCommands.SetProperty> commit;

    private Property(Commit<GroupCommands.SetProperty> commit) {
      this.commit = commit;
    }

    /**
     * Returns the property name.
     */
    public String name() {
      return commit.operation().property();
    }

    /**
     * Returns the property value.
     */
    public Object value() {
      return commit.operation().value();
    }

    @Override
    public void close() {
      commit.close();
    }
  }

  /**
   * Represents a group task.
   */
  private class Task implements AutoCloseable {
    private final Commit<GroupCommands.Submit> commit;

    private Task(Commit<GroupCommands.Submit> commit) {
      this.commit = commit;
    }

    /**
     * Returns the task ID.
     */
    public long id() {
      return commit.operation().id();
    }

    /**
     * Returns the task index.
     */
    public long index() {
      return commit.index();
    }

    /**
     * Returns a boolean indicating whether this is a direct task.
     */
    public boolean direct() {
      return commit.operation().member() != null;
    }

    /**
     * Returns the task session.
     */
    public ServerSession session() {
      return commit.session();
    }

    /**
     * Returns the task value.
     */
    public Object task() {
      return commit.operation().task();
    }

    /**
     * Returns a boolean indicating whether the task is complete.
     */
    public boolean complete() {
      if (commit.operation().member() == null) {
        for (Member member : members.values()) {
          if (member.task != null && member.task.index() <= index()) {
            return false;
          }
        }
      } else {
        Member member = members.get(commit.operation().member());
        if (member != null) {
          if (member.task != null && member.task.index() <= index()) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     * Sends an ack message back to the task submitter.
     */
    public void ack() {
      if (session().state().active()) {
        session().publish("ack", commit.operation());
      }
    }

    /**
     * Sends a fail message back to the task submitter.
     */
    public void fail() {
      if (session().state().active()) {
        session().publish("fail", commit.operation());
      }
    }

    @Override
    public void close() {
      commit.close();
    }
  }

}
