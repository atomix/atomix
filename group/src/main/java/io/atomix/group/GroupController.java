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
package io.atomix.group;

/**
 * Controls events for an object.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface GroupController {

  /**
   * Returns the underlying group.
   *
   * @return The underlying group.
   */
  DistributedGroup group();

  /**
   * Called when a member joins the group.
   *
   * @param member The member that joined the group.
   */
  void onJoin(GroupMember member);

  /**
   * Called when a member leaves the group.
   *
   * @param member The member that left the group.
   */
  void onLeave(GroupMember member);

}
