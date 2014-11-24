/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.event;

import net.kuujo.copycat.cluster.ClusterMember;

/**
 * Leader elect event.<p>
 *
 * This event will be triggered when the local node is elected leader or discovers a newly elected
 * cluster leader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderElectEvent implements Event {
  private final long term;
  private final ClusterMember leader;

  public LeaderElectEvent(long term, ClusterMember leader) {
    this.term = term;
    this.leader = leader;
  }

  /**
   * Returns the leader's term.
   *
   * @return The leader's term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the leader URI.
   *
   * @return The leader URI.
   */
  public ClusterMember leader() {
    return leader;
  }

  @Override
  public String toString() {
    return String.format("LeaderElectEvent[term=%d, leader=%s]", term, leader);
  }

}
