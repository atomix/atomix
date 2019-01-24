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
package io.atomix.core.election;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import io.atomix.cluster.MemberId;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * State of leadership for topic.
 * <p>
 * Provided by this construct is the current {@link Leader leader} and the list of
 * {@link MemberId memberId}s currently registered as candidates for election for the topic.
 * Keep in mind that only registered candidates can become leaders.
 */
public class Leadership<T> {

  private final Leader<T> leader;
  private final List<T> candidates;

  public Leadership(Leader<T> leader, List<T> candidates) {
    this.leader = leader;
    this.candidates = ImmutableList.copyOf(candidates);
  }

  /**
   * Returns the leader for this topic.
   *
   * @return leader; will be null if there is no leader for topic
   */
  public Leader<T> leader() {
    return leader;
  }

  /**
   * Returns an preference-ordered list of nodes that are in the leadership
   * race for this topic.
   *
   * @return a list of NodeIds in priority-order, or an empty list.
   */
  public List<T> candidates() {
    return candidates;
  }

  /**
   * Maps the leadership identifiers using the given mapper.
   *
   * @param mapper the mapper with which to convert identifiers
   * @param <U>    the converted identifier type
   * @return the converted leadership
   */
  public <U> Leadership<U> map(Function<T, U> mapper) {
    return new Leadership<>(
        leader != null ? leader.map(mapper) : null,
        candidates.stream().map(mapper).collect(Collectors.toList()));
  }

  @Override
  public int hashCode() {
    return Objects.hash(leader, candidates);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof Leadership) {
      final Leadership other = (Leadership) obj;
      return Objects.equals(this.leader, other.leader)
          && Objects.equals(this.candidates, other.candidates);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("leader", leader)
        .add("candidates", candidates)
        .toString();
  }
}
