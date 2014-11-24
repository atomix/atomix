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

import net.kuujo.copycat.cluster.Cluster;

/**
 * Cluster membership change event.<p>
 *
 * This event will be triggered when the local node <em>commits</em> a cluster configuration change. Note
 * that this does not necessarily indicate that the user-provided cluster configuration changed. Copycat
 * maintains a separate internal replicated cluster configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MembershipChangeEvent implements Event {
  private final Cluster cluster;

  public MembershipChangeEvent(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Returns the cluster whose membership changed.
   */
  public Cluster cluser() {
    return cluster;
  }
  
  @Override
  public String toString() {
    return String.format("MembershipChangeEvent[cluster=%s]", cluster);
  }

}
