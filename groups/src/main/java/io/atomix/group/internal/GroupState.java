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
package io.atomix.group.internal;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.group.task.FailoverStrategy;
import io.atomix.group.task.RoutingStrategy;
import io.atomix.group.task.internal.GroupTask;
import io.atomix.resource.ResourceStateMachine;

import java.time.Duration;
import java.util.*;

/**
 * Group state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class GroupState extends ResourceStateMachine implements SessionListener {
  private final Random random = new Random(141650939l);
  private final Duration expiration;
  private final Map<Long, GroupSession> sessions = new HashMap<>();
  private final Map<String, Member> members = new HashMap<>();
  private final List<Member> membersList = new ArrayList<>();
  private final List<Member> candidates = new ArrayList<>();
  private Member leader;
  private long term;

  public GroupState(Properties config) {
    super(config);
    expiration = Duration.ofMillis(Long.valueOf(config.getProperty("expiration", "0")));
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
      if (member.session() != null && member.session().equals(session)) {
        // If the member is not persistent, remove the member from the membership group.
        if (!member.persistent()) {
          iterator.remove();
          membersList.remove(member);
          candidates.remove(member);
          left.put(member.index(), member);
        } else {
          // If the member is persistent, set its session to null to exclude it from events.
          member.setSession(null);
          candidates.remove(member);

          // For persistent members, if the expiration duration is non-zero then we wait the prescribed duration before
          // sending a leave event to the remaining sessions, and only send a leave event if the member is still dead.
          if (expiration.isZero()) {
            sessions.values().forEach(s -> s.leave(member));
          } else {
            executor.schedule(expiration, () -> {
              if (member.session() == null) {
                sessions.values().forEach(s -> s.leave(member));
              }
            });
          }
        }
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
    if (candidates.isEmpty())
      return;

    Random random = new Random(term);
    Member member = candidates.remove(random.nextInt(candidates.size()));
    while (member != null) {
      if (!member.session().state().active()) {
        if (!candidates.isEmpty()) {
          member = candidates.remove(random.nextInt(candidates.size()));
        } else {
          break;
        }
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
        membersList.add(member);
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
        // Update the member's session to the commit session the member may have been reopened via a new session.
        member.setSession(commit.session());

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
        membersList.remove(member);
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
        if (member.session() != null && member.session().state().active()) {
          members.add(member.info());
        }
      }
      return members;
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
        // Create a task instance.
        Task task = new Task(commit);

        // Ensure that the member is a member of the group.
        Member member = members.get(commit.operation().member());
        if (member == null) {
          task.fail();
          task.close();
        } else {
          // Add the task to the member's task queue.
          member.submit(task);
        }
      } else if (commit.operation().routingStrategy() == RoutingStrategy.DIRECT) {
        // Create a task instance.
        Task task = new Task(commit);

        // If the members list is empty, fail the task submission.
        if (members.isEmpty()) {
          task.fail();
          task.close();
        } else {
          Member member = membersList.get(random.nextInt(membersList.size()));

          // Add the task to the member's task queue.
          member.submit(task);
        }
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

  @Override
  public void delete() {
    members.values().forEach(Member::close);
    members.clear();
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
        session.publish("join", member.info());
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
  private class Member implements AutoCloseable {
    private final Commit<GroupCommands.Join> commit;
    private final long index;
    private final String memberId;
    private final Address address;
    private final boolean persistent;
    private ServerSession session;
    private final Queue<Task> tasks = new ArrayDeque<>();
    private Task task;

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
      return new GroupMemberInfo(index, memberId, address);
    }

    /**
     * Returns the member session.
     */
    public ServerSession session() {
      return session;
    }

    /**
     * Sets the member session.
     */
    public void setSession(ServerSession session) {
      this.session = session;
      if (task != null && session != null && session.state().active()) {
        session.publish("task", new GroupTask<>(task.index(), memberId, task.type(), task.task()));
      }
    }

    /**
     * Returns a boolean indicating whether the member is persistent.
     */
    public boolean persistent() {
      return persistent;
    }

    /**
     * Submits the given task to be processed by the member.
     */
    public void submit(Task task) {
      if (this.task == null) {
        this.task = task;
        if (session != null && session.state().active()) {
          session.publish("task", new GroupTask<>(task.index(), memberId, task.type(), task.task()));
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
        if (task.complete()) {
          task.ack();
          task.close();
        }
        next();
      }
    }

    /**
     * Fails processing of a task.
     */
    public void fail(long id) {
      if (this.task.index() == id) {
        Task task = this.task;
        this.task = null;
        if (task.direct()) {
          task.fail();
          task.close();
        } else if (task.complete()) {
          task.ack();
          task.close();
        }
        next();
      }
    }

    /**
     * Sends the next task in the queue.
     */
    private void next() {
      task = tasks.poll();
      if (task != null) {
        if (session != null && session.state().active()) {
          session.publish("task", new GroupTask<>(task.index(), memberId, task.type(), task.task()));
        }
      }
    }

    @Override
    public void close() {
      Task task = this.task;
      this.task = null;
      if (task != null) {
        if (task.commit.operation().routingStrategy() == RoutingStrategy.DIRECT && task.commit.operation().failoverStrategy() == FailoverStrategy.RESUBMIT) {
          if (!members.isEmpty()) {
            Member member = membersList.get(random.nextInt(membersList.size()));
            member.submit(task);
          } else {
            task.fail();
            task.close();
          }
        } else {
          task.fail();
          task.close();
        }
      }

      tasks.forEach(t -> {
        if (t.commit.operation().routingStrategy() == RoutingStrategy.DIRECT && task.commit.operation().failoverStrategy() == FailoverStrategy.RESUBMIT) {
          if (!members.isEmpty()) {
            Member member = membersList.get(random.nextInt(membersList.size()));
            member.submit(t);
          } else {
            t.fail();
            t.close();
          }
        } else {
          t.fail();
          t.close();
        }
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
     * Returns the task type.
     */
    public String type() {
      return commit.operation().type();
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
