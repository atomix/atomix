/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.election;

import io.atomix.group.GroupMember;

/**
 * Represents a unique leader election term.
 * <p>
 * Each time a leader is elected in a group, it's elected for a unique, monotonically increasing {@link #term() term}.
 * The elected leader will persist until it becomes disconnected from the cluster or explicitly leaves the group.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Term {

  /**
   * Returns the term number.
   * <p>
   * The returned term number is guaranteed to be unique to this instance throughout the lifetime of the group
   * and is guaranteed to be greater than all prior terms (monotonically increasing).
   *
   * @return The election term number.
   */
  long term();

  /**
   * Returns the leader for the term.
   * <p>
   * If no leader has been elected for the term, the returned leader will be {@code null}. Once a leader has been
   * elected, the leader is guaranteed to persist for the remainder of this term until the leader either leaves the
   * group or becomes disconnected from the cluster.
   *
   * @return The leader for this term or {@code null} if no leader has been elected yet.
   */
  GroupMember leader();

}
