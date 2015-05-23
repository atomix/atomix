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

import java.util.concurrent.TimeUnit;

/**
 * Raft status configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class RaftConfig {
  private static final long DEFAULT_RAFT_ELECTION_TIMEOUT = 500;
  private static final long DEFAULT_RAFT_HEARTBEAT_INTERVAL = 150;

  private long electionTimeout = DEFAULT_RAFT_ELECTION_TIMEOUT;
  private long heartbeatInterval = DEFAULT_RAFT_HEARTBEAT_INTERVAL;

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

}
