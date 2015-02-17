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

import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.internal.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Raft status configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftConfig implements Configurable {
  private static final String RAFT_ID = "id";
  private static final String RAFT_NAME = "name";
  private static final String RAFT_ADDRESS = "address";
  private static final String RAFT_ELECTION_TIMEOUT = "election.timeout";
  private static final String RAFT_HEARTBEAT_INTERVAL = "heartbeat.interval";
  private static final String RAFT_MEMBERS = "members";
  private static final String RAFT_LOG = "log";

  private static final long DEFAULT_RAFT_ELECTION_TIMEOUT = 500;
  private static final long DEFAULT_RAFT_HEARTBEAT_INTERVAL = 150;
  private final Log DEFAULT_RAFT_LOG = new BufferedLog();

  private Map<String, Object> config;

  public RaftConfig() {
    this.config = new HashMap<>(128);
  }

  public RaftConfig(Map<String, Object> config) {
    this.config = config;
  }

  protected RaftConfig(RaftConfig config) {
    this.config = new HashMap<>(config.toMap());
  }

  @Override
  public RaftConfig copy() {
    return new RaftConfig(this);
  }

  @Override
  public void configure(Map<String, Object> config) {
    this.config = config;
  }

  @Override
  public Map<String, Object> toMap() {
    return null;
  }

  /**
   * Sets the local member's unique ID.
   *
   * @param id The local member's unique ID.
   * @throws java.lang.NullPointerException If the ID is {@code null}
   */
  public void setId(String id) {
    config.put(RAFT_ID, Assert.notNull(id, "id"));
  }

  /**
   * Returns the local member's unique ID.
   *
   * @return The local member's unique ID.
   */
  public String getId() {
    return (String) config.get(RAFT_ID);
  }

  /**
   * Sets the local member's unique ID, returning the configuration for method chaining.
   *
   * @param id The local member's unique ID.
   * @return The Raft configuration.
   * @throws java.lang.NullPointerException If the ID is {@code null}
   */
  public RaftConfig withId(String id) {
    setId(id);
    return this;
  }

  /**
   * Sets the Raft algorithm name.
   *
   * @param name The algorithm name.
   * @throws java.lang.NullPointerException If the name is {@code null}
   */
  public void setName(String name) {
    config.put(RAFT_NAME, Assert.notNull(name, "name"));
  }

  /**
   * Returns the Raft algorithm name.
   *
   * @return The Raft algorithm name.
   * @throws java.lang.NullPointerException If the algorithm name has not been configured.
   */
  public String getName() {
    return (String) config.get(RAFT_NAME);
  }

  /**
   * Sets the Raft algorithm name, returning the configuration for method chaining.
   *
   * @param name The Raft algorithm name.
   * @return The Raft configuration.
   */
  public RaftConfig withName(String name) {
    setName(name);
    return this;
  }

  /**
   * Sets the local member address.
   *
   * @param address The local member address.
   * @throws java.lang.NullPointerException If the address is {@code null}
   */
  public void setAddress(String address) {
    config.put(RAFT_ADDRESS, Assert.notNull(address, "address"));
  }

  /**
   * Returns the local member address.
   *
   * @return The local member address.
   */
  public String getAddress() {
    return (String) config.get(RAFT_ADDRESS);
  }

  /**
   * Sets the local member address, returning the configuration for method chaining.
   *
   * @param address The local member address.
   * @return The Raft configuration.
   * @throws java.lang.NullPointerException If the address is {@code null}
   */
  public RaftConfig withAddress(String address) {
    setAddress(address);
    return this;
  }

  /**
   * Sets the Raft election timeout.
   *
   * @param electionTimeout The Raft election timeout in milliseconds.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public void setElectionTimeout(long electionTimeout) {
    config.put(RAFT_ELECTION_TIMEOUT, Assert.arg(electionTimeout, Assert.POSITIVE, "election timeout must be positive"));
  }

  /**
   * Sets the Raft election timeout.
   *
   * @param electionTimeout The Raft election timeout.
   * @param unit The timeout unit.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
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
    Long electionTimeout = (Long) config.get(RAFT_ELECTION_TIMEOUT);
    return electionTimeout != null ? electionTimeout : DEFAULT_RAFT_ELECTION_TIMEOUT;
  }

  /**
   * Sets the Raft election timeout, returning the Raft configuration for method chaining.
   *
   * @param electionTimeout The Raft election timeout in milliseconds.
   * @return The Raft configuration.
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
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
   * @throws java.lang.IllegalArgumentException If the election timeout is not positive
   */
  public RaftConfig withElectionTimeout(long electionTimeout, TimeUnit unit) {
    setElectionTimeout(electionTimeout, unit);
    return this;
  }

  /**
   * Sets the Raft heartbeat interval.
   *
   * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
   */
  public void setHeartbeatInterval(long heartbeatInterval) {
    config.put(RAFT_HEARTBEAT_INTERVAL, Assert.arg(heartbeatInterval, Assert.POSITIVE, "heartbeat interval must be positive"));
  }

  /**
   * Sets the Raft heartbeat interval.
   *
   * @param heartbeatInterval The Raft heartbeat interval.
   * @param unit The heartbeat interval unit.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
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
    Long heartbeatInterval = (Long) config.get(RAFT_HEARTBEAT_INTERVAL);
    return heartbeatInterval != null ? heartbeatInterval : DEFAULT_RAFT_HEARTBEAT_INTERVAL;
  }

  /**
   * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
   *
   * @param heartbeatInterval The Raft heartbeat interval in milliseconds.
   * @return The Raft configuration.
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
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
   * @throws java.lang.IllegalArgumentException If the heartbeat interval is not positive
   */
  public RaftConfig withHeartbeatInterval(long heartbeatInterval, TimeUnit unit) {
    setHeartbeatInterval(heartbeatInterval, unit);
    return this;
  }

  /**
   * Sets the map of Raft seed members.
   *
   * @param members A map of Raft seed member IDs to member addresses.
   * @throws java.lang.NullPointerException If the map is {@code null}
   */
  public void setMembers(Map<String, String> members) {
    config.put(RAFT_MEMBERS, Assert.notNull(members, "members"));
  }

  /**
   * Returns the map of Raft seed members.
   *
   * @return A map of Raft seed member IDs to member addresses.
   */
  @SuppressWarnings("unchecked")
  public Map<String, String> getMembers() {
    Map<String, String> members = (Map<String, String>) config.get(RAFT_MEMBERS);
    if (members == null) {
      members = new HashMap<>();
      config.put(RAFT_MEMBERS, members);
    }
    return members;
  }

  /**
   * Sets the map of Raft seed members, returning the configuration for method chaining.
   *
   * @param members The map of Raft seed members.
   * @return The Raft configuration.
   * @throws java.lang.NullPointerException If the map is {@code null}
   */
  public RaftConfig withMembers(Map<String, String> members) {
    setMembers(members);
    return this;
  }

  /**
   * Adds a member to the map of Raft seed members.
   *
   * @param id The unique seed member ID.
   * @param address The seed member address.
   * @return The Raft configuration.
   * @throws java.lang.NullPointerException If the member ID or address is {@code null}
   */
  @SuppressWarnings("unchecked")
  public RaftConfig addMember(String id, String address) {
    Map<String, String> members = (Map<String, String>) config.get(RAFT_MEMBERS);
    if (members == null) {
      members = new HashMap<>();
      config.put(RAFT_MEMBERS, members);
    }
    members.put(Assert.notNull(id, "id"), Assert.notNull(address, "address"));
    return this;
  }

  /**
   * Removes a member from the map of Raft seed members.
   *
   * @param id The unique seed member ID.
   * @return The Raft configuration.
   * @throws java.lang.NullPointerException If the member ID is {@code null}
   */
  @SuppressWarnings("unchecked")
  public RaftConfig removeMember(String id) {
    Map<String, String> members = (Map<String, String>) config.get(RAFT_MEMBERS);
    if (members == null) {
      members = new HashMap<>();
      config.put(RAFT_MEMBERS, members);
    }
    members.remove(Assert.notNull(id, "id"));
    return this;
  }

  /**
   * Clears the set of members.
   *
   * @return The Raft configuration.
   */
  public RaftConfig clearMembers() {
    config.remove(RAFT_MEMBERS);
    return this;
  }

  /**
   * Sets the Raft log.
   *
   * @param log The Raft log.
   * @throws java.lang.NullPointerException If the {@code log} is {@code null}
   */
  public void setLog(Log log) {
    config.put(RAFT_LOG, Assert.notNull(log, "log").toMap());
  }

  /**
   * Returns the Raft log.
   *
   * @return The Raft log.
   */
  @SuppressWarnings("unchecked")
  public Log getLog() {
    Map<String, Object> log = (Map<String, Object>) config.get(RAFT_LOG);
    return log != null ? Configurable.load(log) : DEFAULT_RAFT_LOG;
  }

  /**
   * Sets the Raft log, returning the Raft configuration for method chaining.
   *
   * @param log The Raft log.
   * @return The Raft configuration.
   * @throws java.lang.NullPointerException If the {@code log} is {@code null}
   */
  public RaftConfig withLog(Log log) {
    setLog(log);
    return this;
  }

}
