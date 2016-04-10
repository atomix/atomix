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
package io.atomix.group.election.internal;

import io.atomix.group.GroupMember;
import io.atomix.group.election.Term;

/**
 * Group term.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class GroupTerm implements Term {
  private final long term;
  private volatile GroupMember leader;

  public GroupTerm(long term) {
    this.term = term;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public GroupMember leader() {
    return leader;
  }

  /**
   * Sets the term leader.
   *
   * @param leader The term leader.
   */
  void setLeader(GroupMember leader) {
    this.leader = leader;
  }

}
