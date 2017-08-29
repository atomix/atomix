/*
 * Copyright 2014-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.leadership;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.atomix.cluster.NodeId;
import io.atomix.event.ListenerService;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Service for leader election.
 * <p>
 * Leadership contests are organized around topics. A instance can join the
 * leadership race for a topic or withdraw from a race it has previously joined.
 * <p>
 * Listeners can be added to receive notifications asynchronously for various
 * leadership contests.
 * <p>
 * When a node gets elected as a leader for a topic, all nodes receive notifications
 * indicating a change in leadership.
 */
public interface LeadershipService
    extends ListenerService<LeadershipEvent, LeadershipEventListener> {

  /**
   * Returns the {@link NodeId node identifier} that is the current leader for a topic.
   *
   * @param topic leadership topic
   * @return node identifier of the current leader; {@code null} if there is no leader for the topic
   */
  default NodeId getLeader(String topic) {
    Leadership leadership = getLeadership(topic);
    return leadership == null ? null : leadership.leaderNodeId();
  }

  /**
   * Returns the current {@link Leadership leadership} for a topic.
   *
   * @param topic leadership topic
   * @return leadership or {@code null} if no such topic exists
   */
  Leadership getLeadership(String topic);

  /**
   * Returns the set of topics owned by the specified {@link NodeId node}.
   *
   * @param nodeId node identifier.
   * @return set of topics for which this node is the current leader.
   */
  default Set<String> ownedTopics(NodeId nodeId) {
    return Maps.filterValues(getLeaderBoard(), v -> Objects.equal(nodeId, v.leaderNodeId())).keySet();
  }

  /**
   * Enters a leadership contest.
   *
   * @param topic leadership topic
   * @return {@code Leadership} future
   */
  Leadership runForLeadership(String topic);

  /**
   * Withdraws from a leadership contest.
   *
   * @param topic leadership topic
   */
  void withdraw(String topic);

  /**
   * Returns the current leader board.
   *
   * @return mapping from topic to leadership info.
   * @deprecated 1.6.0 Goldeneye release. Replace usages with {@link #getLeadership(String)}
   */
  @Deprecated
  Map<String, Leadership> getLeaderBoard();

  /**
   * Returns the candidate nodes for each topic.
   *
   * @return A mapping from topics to corresponding list of candidates.
   * @deprecated 1.6.0 Goldeneye release. Replace usages with {@link #getLeadership(String)}
   */
  @Deprecated
  default Map<String, List<NodeId>> getCandidates() {
    return ImmutableMap.copyOf(Maps.transformValues(getLeaderBoard(), v -> ImmutableList.copyOf(v.candidates())));
  }

  /**
   * Returns the candidate nodes for a given topic.
   *
   * @param topic leadership topic
   * @return A lists of {@link NodeId nodeIds}, which may be empty.
   */
  default List<NodeId> getCandidates(String topic) {
    Leadership leadership = getLeadership(topic);
    return leadership == null ? ImmutableList.of() : ImmutableList.copyOf(leadership.candidates());
  }
}