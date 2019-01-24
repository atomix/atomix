/*
 * Copyright 2016-present Open Networking Foundation
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
import com.google.common.base.Objects;
import io.atomix.cluster.MemberId;

import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Topic leader.
 * <p>
 * Identified by the {@link MemberId node identifier} and a monotonically increasing term number.
 * The term number is incremented by one every time a new node is elected as leader.
 * Also available is the system clock time at the instant when this node was elected as leader.
 * Keep in mind though that as with any system clock based time stamps this particular information
 * susceptible to clock skew and should only be relied on for simple diagnostic purposes.
 */
public class Leader<T> {
  private final T id;
  private final long term;
  private final long termStartTime;

  public Leader(T id, long term, long termStartTime) {
    this.id = checkNotNull(id);
    checkArgument(term >= 0, "term must be non-negative");
    this.term = term;
    checkArgument(termStartTime >= 0, "termStartTime must be non-negative");
    this.termStartTime = termStartTime;
  }

  /**
   * Returns the identifier for of leader.
   *
   * @return node identifier
   */
  public T id() {
    return id;
  }

  /**
   * Returns the leader's term.
   *
   * @return leader term
   */
  public long term() {
    return term;
  }

  /**
   * Returns the system time when the current leadership term started.
   *
   * @return current leader term start time
   */
  public long timestamp() {
    return termStartTime;
  }

  /**
   * Converts the leader identifier using the given mapping function.
   *
   * @param mapper the mapping function with which to convert the identifier
   * @param <U>    the converted type
   * @return the converted leader object
   */
  public <U> Leader<U> map(Function<T, U> mapper) {
    return new Leader<>(mapper.apply(id), term, termStartTime);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other != null && other instanceof Leader) {
      Leader that = (Leader) other;
      return Objects.equal(this.id, that.id)
          && this.term == that.term
          && this.termStartTime == that.termStartTime;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, term, termStartTime);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("id", id)
        .add("term", term)
        .add("termStartTime", termStartTime)
        .toString();
  }
}
