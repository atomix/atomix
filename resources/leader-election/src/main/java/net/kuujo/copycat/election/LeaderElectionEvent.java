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
package net.kuujo.copycat.election;

import net.kuujo.copycat.Event;
import net.kuujo.copycat.cluster.Cluster;

/**
 * Leader election event.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderElectionEvent implements Event {
  private final Cluster cluster;

  public LeaderElectionEvent(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Returns the resource's cluster.
   *
   * @return The leader election resource's cluster.
   */
  public Cluster cluster() {
    return cluster;
  }

}
