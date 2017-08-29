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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import io.atomix.cluster.NodeId;

import java.util.List;
import java.util.Objects;

/**
 * State of leadership for topic.
 * <p>
 * Provided by this construct is the current {@link Leader leader} and the list of
 * {@link NodeId nodeId}s currently registered as candidates for election for the topic.
 * Keep in mind that only registered candidates can become leaders.
 */
public class Leadership {

  private final String topic;
  private final Leader leader;
  private final List<NodeId> candidates;

  public Leadership(String topic, Leader leader, List<NodeId> candidates) {
    this.topic = topic;
    this.leader = leader;
    this.candidates = ImmutableList.copyOf(candidates);
  }

  /**
   * Returns the leadership topic.
   *
   * @return leadership topic.
   */
  public String topic() {
    return topic;
  }

  /**
   * Returns the {@link NodeId nodeId} of the leader.
   *
   * @return leader node identifier; will be null if there is no leader
   */
  public NodeId leaderNodeId() {
    return leader == null ? null : leader.nodeId();
  }

  /**
   * Returns the leader for this topic.
   *
   * @return leader; will be null if there is no leader for topic
   */
  public Leader leader() {
    return leader;
  }

  /**
   * Returns an preference-ordered list of nodes that are in the leadership
   * race for this topic.
   *
   * @return a list of NodeIds in priority-order, or an empty list.
   */
  public List<NodeId> candidates() {
    return candidates;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, leader, candidates);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof Leadership) {
      final Leadership other = (Leadership) obj;
      return Objects.equals(this.topic, other.topic) &&
          Objects.equals(this.leader, other.leader) &&
          Objects.equals(this.candidates, other.candidates);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("topic", topic)
        .add("leader", leader)
        .add("candidates", candidates)
        .toString();
  }
}
