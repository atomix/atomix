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

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.resource.ResourceStateMachine;

import java.time.Duration;
import java.util.*;

/**
 * Group state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class GroupState extends ResourceStateMachine implements SessionListener {
  private final Duration expiration;
  private final Map<Long, SessionState> sessions = new HashMap<>();
  private final MembersState members = new MembersState();
  private final Map<String, QueueState> queues = new HashMap<>();
  private final List<MemberState> candidates = new ArrayList<>();
  private MemberState leader;
  private long term;

  public GroupState(Properties config) {
    super(config);
    expiration = Duration.ofMillis(Long.valueOf(config.getProperty("expiration", "0")));
  }

  @Override
  public void close(ServerSession session) {
    Map<Long, MemberState> left = new HashMap<>();

    // Remove the session from the sessions set.
    sessions.remove(session.id());

    // Iterate through all open members.
    Iterator<MemberState> iterator = members.iterator();
    while (iterator.hasNext()) {
      // If the member is associated with the closed session, remove it from the members list.
      MemberState member = iterator.next();
      if (member.session() != null && member.session().equals(session)) {
        // If the member is not persistent, remove the member from the membership group.
        if (!member.persistent()) {
          iterator.remove();
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
    MemberState member = candidates.remove(random.nextInt(candidates.size()));
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
      MemberState member = members.get(commit.operation().member());

      // If the member doesn't already exist, create it.
      if (member == null) {
        member = new MemberState(commit);

        // Store the member ID and join commit mappings and add the member as a candidate.
        members.add(member);
        candidates.add(member);

        // Iterate through available sessions and publish a join event to each session.
        for (SessionState session : sessions.values()) {
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
        for (SessionState session : sessions.values()) {
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
      MemberState member = members.remove(commit.operation().member());
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
      sessions.put(commit.session().id(), new SessionState(commit.session()));
      Set<GroupMemberInfo> members = new HashSet<>();
      for (MemberState member : this.members) {
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
  public void send(Commit<GroupCommands.Message> commit) {
    try {
      QueueState queue = queues.computeIfAbsent(commit.operation().queue(), t -> new QueueState(members));
      switch (commit.operation().deliveryPolicy()) {
        case SYNC:
          queue.submit(new SyncMessageState(commit, queue));
          break;
        case ASYNC:
          queue.submit(new AsyncMessageState(commit, queue));
          break;
        case REQUEST_REPLY:
          queue.submit(new RequestReplyMessageState(commit, queue));
          break;
        default:
          commit.close();
          throw new IllegalArgumentException("unknown delivery policy");
      }
    } catch (Exception e) {
      commit.close();
      throw e;
    }
  }

  /**
   * Handles a reply commit.
   */
  public void reply(Commit<GroupCommands.Reply> commit) {
    try {
      QueueState queue = queues.get(commit.operation().queue());
      if (queue != null) {
        queue.reply(commit.operation().id(), commit.operation().member(), commit.operation().message());
      }
    } finally {
      commit.close();
    }
  }

  @Override
  public void delete() {
    queues.values().forEach(QueueState::close);
    members.close();
  }

}
