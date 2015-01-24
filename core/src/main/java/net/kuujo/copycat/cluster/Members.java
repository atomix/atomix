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
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.EventListener;

import java.util.Collection;

/**
 * Listenable collection of cluster members.<p>
 *
 * This is a simple collection implementation that supports adding membership listeners to the set of cluster members.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Members extends Collection<Member> {

  /**
   * Adds a membership listener to the members set.<p>
   *
   * Membership listeners are triggered when {@link Member.Type#PASSIVE} members join or leave the cluster. Copycat uses
   * a gossip based failure detection algorithm to detect failures, using vector clocks to version cluster
   * configurations. In order to prevent false positives due to network partitions, Copycat's failure detection
   * algorithm will attempt to contact a member from up to three different nodes before considering that node failed.
   * If the membership listener is called with a {@link MembershipEvent.Type#LEAVE} event, that indicates that Copycat
   * has attempted to contact the missing member multiple times.<p>
   *
   * {@link Member.Type#ACTIVE} members never join or leave the cluster since they are explicitly configured, active,
   * voting members of the cluster. However, this may change at some point in the future to allow failure detection for
   * active members as well.
   *
   * @param listener The membership event listener to add.
   * @return The members set.
   */
  Members addListener(EventListener<MembershipEvent> listener);

  /**
   * Removes a membership listener from the members set.
   *
   * @param listener The membership event listener to remove.
   * @return The members set.
   */
  Members removeListener(EventListener<MembershipEvent> listener);

}
