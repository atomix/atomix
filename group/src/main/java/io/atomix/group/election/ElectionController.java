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

import io.atomix.group.DistributedGroup;
import io.atomix.group.GroupMember;

/**
 * Group election controller.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ElectionController {
  private final Election election;

  public ElectionController(DistributedGroup group) {
    this.election = new Election(group);
  }

  /**
   * Returns the underlying election.
   *
   * @return The underlying election.
   */
  public Election election() {
    return election;
  }

  /**
   * Called when a member joins the election.
   */
  public synchronized void onJoin(GroupMember member) {
    if (election.term() == null || election.term().term() != election.term().leader().version()) {
      election.elect();
    }
  }

  /**
   * Called when a member leaves the election.
   */
  public synchronized void onLeave(GroupMember member) {
    if (election.term() != null && election.term().leader().equals(member)) {
      election.elect();
    }
  }

}
