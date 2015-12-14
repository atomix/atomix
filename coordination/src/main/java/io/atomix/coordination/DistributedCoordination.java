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
 * limitations under the License
 */
package io.atomix.coordination;

import io.atomix.resource.ResourceType;

/**
 * Distributed coordination resource types.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class DistributedCoordination {

  private DistributedCoordination() {
  }

  /**
   * Distributed leader election resource.
   */
  public static final ResourceType<DistributedLeaderElection> LEADER_ELECTION = DistributedLeaderElection.TYPE;

  /**
   * Distributed lock resource.
   */
  public static final ResourceType<DistributedLock> LOCK = DistributedLock.TYPE;

  /**
   * Distributed membership group resource.
   */
  public static final ResourceType<DistributedMembershipGroup> MEMBERSHIP_GROUP = DistributedMembershipGroup.TYPE;

  /**
   * Distributed message bus resource.
   */
  public static final ResourceType<DistributedMessageBus> MESSAGE_BUS = DistributedMessageBus.TYPE;

}
