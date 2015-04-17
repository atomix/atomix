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
package net.kuujo.copycat.election;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.cluster.ElectionEvent;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.resource.internal.AbstractDiscreteResource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Leader election.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaderElection extends AbstractDiscreteResource<LeaderElection> {
  private final Map<EventListener<Member>, EventListener<ElectionEvent>> listeners = new HashMap<>();

  public LeaderElection(LeaderElectionConfig config, ClusterConfig cluster) {
    super(config, cluster);
  }

  public LeaderElection(LeaderElectionConfig config, ClusterConfig cluster, Executor executor) {
    super(config, cluster, executor);
  }

  /**
   * Registers a leader election listener.
   *
   * @param listener The leader election listener.
   * @return The leader election.
   */
  public synchronized LeaderElection addListener(EventListener<Member> listener) {
    if (!listeners.containsKey(listener)) {
      EventListener<ElectionEvent> wrapper = event -> listener.accept(event.winner());
      listeners.put(listener, wrapper);
      context.getCluster().addElectionListener(wrapper);
    }
    return this;
  }

  /**
   * Removes a leader election listener.
   *
   * @param listener The leader election listener.
   * @return The leader election.
   */
  public synchronized LeaderElection removeListener(EventListener<Member> listener) {
    EventListener<ElectionEvent> wrapper = listeners.remove(listener);
    if (wrapper != null) {
      context.getCluster().removeElectionListener(wrapper);
    }
    return this;
  }

}
