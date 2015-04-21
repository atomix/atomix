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
package net.kuujo.copycat.raft;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Raft status configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftConfig {
  private static final long DEFAULT_RAFT_ELECTION_TIMEOUT = 500;
  private static final long DEFAULT_RAFT_HEARTBEAT_INTERVAL = 150;

  private int id;
  private RaftMember.Type type;
  private long electionTimeout = DEFAULT_RAFT_ELECTION_TIMEOUT;
  private long heartbeatInterval = DEFAULT_RAFT_HEARTBEAT_INTERVAL;
  private Set<Integer> members = new HashSet<>(128);

  /**
   * Sets the local member's unique ID.
   *
   * @param id The local member's unique ID.
   * @throws NullPointerException If the ID is {@code null}
   */
  public void setMemberId(int id) {
    if (id <= 0)
      throw new IllegalArgumentException("id cannot be negative");
    this.id = id;
  }

  /**
   * Returns the local member's unique ID.
   *
   * @return The local member's unique ID.
   */
  public int getMemberId() {
    return id;
  }

  /**
   * Sets the local member's unique ID, returning the configuration for method chaining.
   *
   * @param id The local member's unique ID.
   * @return The Raft configuration.
   * @throws NullPointerException If the ID is {@code null}
   */
  public RaftConfig withMemberId(int id) {
    setMemberId(id);
    return this;
  }

  /**
   * Sets the local member type.
   *
   * @param type The local member type.
   * @throws NullPointerException If the member type is {@code null}
   */
  public void setMemberType(RaftMember.Type type) {
    if (type == null)
      throw new NullPointerException("member type cannot be null");
    this.type = type;
  }

  /**
   * Returns the local member type.
   *
   * @return The local member type. Defaults to {@link RaftMember.Type#ACTIVE} if the member is represented in
   *         {@link RaftConfig#getMembers()} or {@link RaftMember.Type#PASSIVE} if not.
   */
  public RaftMember.Type getMemberType() {
    if (members.contains(id))
      return RaftMember.Type.ACTIVE;
    if (type != null)
      return type;
    return RaftMember.Type.PASSIVE;
  }

  /**
   * Sets the local member type, returning the configuration for method chaining.
   *
   * @param type The local member type.
   * @return The Raft configuration.
   * @throws NullPointerException If the member type is {@code null}
   */
  public RaftConfig withMemberType(RaftMember.Type type) {
    setMemberType(type);
    return this;
  }

  /**
   * Sets the Raft election timeout.
   *
   * @param electionTimeout The Raft election timeout in milliseconds.
   * @throws IllegalArgumentException If the election timeout is not positive
   */
  public void setElectionTimeout(long electionTimeout) {
    if (electionTimeout < 1)
      throw new IllegalArgumentException("election timeout must be positive");
    this.electionTimeout = electionTimeout;
  }

  /**
   * Sets the Raft election timeout.
   *
   * @param electionTimeout The Raft election timeout.
   * @param unit The timeout unit.
   * @throws IllegalArgumentException If the election timeout is not positive
   */
  public void setElectionTimeout(long electionTimeout, TimeUnit unit) {
    setElectionTimeout(unit.toMillis(electionTimeout));
  }

  /**
   * Returns the Raft election timeout in milliseconds.
   *
   * @return The Raft election timeout in milliseconds.
   */
  public long getElectionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the Raft election timeout, returning the Raft configuration for method chaining.
   *
   * @param electionTimeout The Raft election timeout in milliseconds.
   * @return The Raft configuration.
   * @throws IllegalArgumentException If the election timeout is not positive
   */
  public RaftConfig withElectionTimeout(long electionTimeout) {
    setElectionTimeout(electionTimeout);
    return this;
  }

  /**
   * Sets the Raft election timeout, returning the Raft configuration for method chaining.
   *
   * @param electionTimeout The Raft election timeout.
   * @param unit The timeout unit.
   * @return The Raft configuration.
   * @throws IllegalArgumentException If the election timeout is not positive
   */
  public RaftConfig withElectionTimeout(long electionTimeout, TimeUnit unit) {
    setElectionTimeout(electionTimeout, unit);
    return this;
  }

  /**
   * Sets the Raft heartbeat interval.
   *
   * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
   * @throws IllegalArgumentException If the heartbeat interval is not positive
   */
  public void setHeartbeatInterval(long heartbeatInterval) {
    if (heartbeatInterval < 1)
      throw new IllegalArgumentException("heartbeat interval must be positive");
    this.heartbeatInterval = heartbeatInterval;
  }

  /**
   * Sets the Raft heartbeat interval.
   *
   * @param heartbeatInterval The Raft heartbeat interval.
   * @param unit The heartbeat interval unit.
   * @throws IllegalArgumentException If the heartbeat interval is not positive
   */
  public void setHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(unit.toMillis(heartbeatInterval));
  }

  /**
   * Returns the Raft heartbeat interval.
   *
   * @return The interval at which nodes send heartbeats to each other.
   */
  public long getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
   *
   * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
   * @return The Raft configuration.
   * @throws IllegalArgumentException If the heartbeat interval is not positive
   */
  public RaftConfig withHeartbeatInterval(long heartbeatInterval) {
    setHeartbeatInterval(heartbeatInterval);
    return this;
  }

  /**
   * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
   *
   * @param heartbeatInterval The Raft heartbeat interval.
   * @param unit The heartbeat interval unit.
   * @return The Raft configuration.
   * @throws IllegalArgumentException If the heartbeat interval is not positive
   */
  public RaftConfig withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(heartbeatInterval, unit);
    return this;
  }

  /**
   * Sets the set of Raft seed members.
   *
   * @param members A set of Raft seed member IDs.
   * @throws NullPointerException If the map is {@code null}
   */
  public void setMembers(int... members) {
    List<Integer> membersList = new ArrayList<>(members.length);
    for (int member : members) {
      membersList.add(member);
    }
    setMembers(membersList);
  }

  /**
   * Sets the set of Raft seed members.
   *
   * @param members A set of Raft seed member IDs.
   * @throws NullPointerException If the map is {@code null}
   */
  public void setMembers(Collection<Integer> members) {
    if (members == null)
      throw new NullPointerException("members cannot be null");
    this.members = new HashSet<>(members);
  }

  /**
   * Returns the set of Raft seed members.
   *
   * @return A set of Raft seed member IDs to member addresses.
   */
  public Set<Integer> getMembers() {
    return members;
  }

  /**
   * Sets the set of Raft seed members, returning the configuration for method chaining.
   *
   * @param members The set of Raft seed members.
   * @return The Raft configuration.
   * @throws NullPointerException If the set is {@code null}
   */
  public RaftConfig withMembers(int... members) {
    List<Integer> membersList = new ArrayList<>(members.length);
    for (int member : members) {
      membersList.add(member);
    }
    setMembers(membersList);
    return this;
  }

  /**
   * Sets the set of Raft seed members, returning the configuration for method chaining.
   *
   * @param members The set of Raft seed members.
   * @return The Raft configuration.
   * @throws NullPointerException If the set is {@code null}
   */
  public RaftConfig withMembers(Collection<Integer> members) {
    setMembers(members);
    return this;
  }

  /**
   * Adds a member to the set of Raft seed members.
   *
   * @param id The unique seed member ID.
   * @return The Raft configuration.
   * @throws NullPointerException If the member ID or address is {@code null}
   */
  @SuppressWarnings("unchecked")
  public RaftConfig addMember(int id) {
    if (id <= 0)
      throw new IllegalArgumentException("id cannot be negative");
    members.add(id);
    return this;
  }

  /**
   * Removes a member from the map of Raft seed members.
   *
   * @param id The unique seed member ID.
   * @return The Raft configuration.
   * @throws NullPointerException If the member ID is {@code null}
   */
  @SuppressWarnings("unchecked")
  public RaftConfig removeMember(int id) {
    if (id <= 0)
      throw new IllegalArgumentException("id cannot be negative");
    members.remove(id);
    return this;
  }

  /**
   * Clears the set of members.
   *
   * @return The Raft configuration.
   */
  public RaftConfig clearMembers() {
    members.clear();
    return this;
  }

}
