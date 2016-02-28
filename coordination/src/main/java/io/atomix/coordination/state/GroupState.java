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

import io.atomix.catalyst.util.hash.Hasher;
import io.atomix.catalyst.util.hash.Murmur2Hasher;
import io.atomix.coordination.DistributedGroup;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.resource.ResourceStateMachine;
import io.atomix.resource.ResourceType;

import java.time.Duration;
import java.util.*;

/**
 * Group state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class GroupState extends ResourceStateMachine implements SessionListener {
  private HashRing hashRing = new HashRing(new Murmur2Hasher(getClass().hashCode()), 32, 0);
  private List<List<String>> partitions = new ArrayList<>(0);
  private final Set<ServerSession> sessions = new HashSet<>();
  private final Map<String, Commit<GroupCommands.Join>> members = new HashMap<>();
  private final Map<String, Map<String, Commit<GroupCommands.SetProperty>>> properties = new HashMap<>();
  private final Queue<Commit<GroupCommands.Join>> candidates = new ArrayDeque<>();
  private Commit<GroupCommands.Join> leader;
  private long term;

  public GroupState() {
    super(new ResourceType(DistributedGroup.class));
  }

  @Override
  public void configure(Properties config) {
    // Configure the hasher.
    Hasher hasher;
    String hasherClass = config.getProperty("hasher", Murmur2Hasher.class.getName());
    try {
      hasher = (Hasher) Thread.currentThread().getContextClassLoader().loadClass(hasherClass).newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    // Set up the hash ring.
    this.hashRing = new HashRing(hasher, Integer.valueOf(config.getProperty("virtualNodes", "100")), Integer.valueOf(config.getProperty("replicationFactor", "0")));

    // Configure the partitions with empty membership lists.
    int numPartitions = Integer.valueOf(config.getProperty("partitions", "1"));
    List<List<String>> partitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(new ArrayList<>());
    }
    this.partitions = partitions;

    // Force the partition membership lists to be populated and existing members to be notified.
    updatePartitions();
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

    for (Map.Entry<Long, Commit<GroupCommands.Join>> entry : left.entrySet()) {
      hashRing.removeMember(entry.getValue().operation().member());
    }
    updatePartitions();

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
      if (session.state() == ServerSession.State.OPEN) {
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
        if (session.state() == ServerSession.State.OPEN) {
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
      if (commit.session().state() == ServerSession.State.EXPIRED || commit.session().state() == ServerSession.State.CLOSED) {
        commit = candidates.poll();
      } else {
        leader = commit;
        for (ServerSession session : sessions) {
          if (session.state() == ServerSession.State.OPEN) {
            session.publish("elect", leader.operation().member());
          }
        }
        break;
      }
    }
  }

  /**
   * Converts an integer to a byte array.
   */
  private byte[] intToByteArray(int value) {
    return new byte[]{(byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value};
  }

  /**
   * Updates partitions.
   */
  private void updatePartitions() {
    List<List<String>> partitions = new ArrayList<>(this.partitions.size());
    for (int i = 0; i < this.partitions.size(); i++) {
      partitions.add(hashRing.members(intToByteArray(i)));
    }
    this.partitions = partitions;

    for (ServerSession session : sessions) {
      if (session.state().active()) {
        session.publish("repartition", partitions);
      }
    }
  }

  /**
   * Applies join commits.
   */
  public String join(Commit<GroupCommands.Join> commit) {
    try {
      String memberId = commit.operation().member();

      // Store the member ID and join commit mappings and add the member as a candidate.
      members.put(memberId, commit);
      candidates.add(commit);

      // Iterate through available sessions and publish a join event to each session.
      for (ServerSession session : sessions) {
        if (session.state() == ServerSession.State.OPEN) {
          session.publish("join", memberId);
        }
      }

      hashRing.addMember(memberId);
      updatePartitions();

      // If the term has not yet been set, set it.
      if (term == 0) {
        incrementTerm();
      }

      // If a leader has not yet been elected, elect one.
      if (leader == null) {
        electLeader();
      }

      return memberId;
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
          if (session.state() == ServerSession.State.OPEN) {
            session.publish("leave", memberId);
          }
        }

        hashRing.removeMember(memberId);
        updatePartitions();

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
  public Set<String> listen(Commit<GroupCommands.Listen> commit) {
    try {
      sessions.add(commit.session());
      return new HashSet<>(members.keySet());
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
    Map<String, Commit<GroupCommands.SetProperty>> properties = this.properties.get(commit.operation().member());
    if (properties == null) {
      properties = new HashMap<>();
      this.properties.put(commit.operation().member(), properties);
    }
    properties.put(commit.operation().property(), commit);
  }

  /**
   * Handles a set property commit.
   */
  public Object getProperty(Commit<GroupCommands.GetProperty> commit) {
    try {
      Map<String, Commit<GroupCommands.SetProperty>> properties = this.properties.get(commit.operation().member());
      if (properties != null) {
        Commit<GroupCommands.SetProperty> value = properties.get(commit.operation().property());
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
  public void removeProperty(Commit<GroupCommands.RemoveProperty> commit) {
    try {
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
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a send commit.
   */
  public void send(Commit<GroupCommands.Send> commit) {
    try {
      Commit<GroupCommands.Join> join = members.get(commit.operation().member());
      if (join == null) {
        throw new IllegalArgumentException("unknown member: " + commit.operation().member());
      }

      join.session().publish("message", new GroupCommands.Message(commit.operation().member(), commit.operation().topic(), commit.operation().message()));
    } finally {
      commit.close();
    }
  }

  /**
   * Handles a schedule commit.
   */
  public void schedule(Commit<GroupCommands.Schedule> commit) {
    try {
      if (!members.containsKey(commit.operation().member())) {
        throw new IllegalArgumentException("unknown member: " + commit.operation().member());
      }

      executor.schedule(Duration.ofMillis(commit.operation().delay()), () -> {
        Commit<GroupCommands.Join> member = members.get(commit.operation().member());
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
  public void execute(Commit<GroupCommands.Execute> commit) {
    try {
      Commit<GroupCommands.Join> member = members.get(commit.operation().member());
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
