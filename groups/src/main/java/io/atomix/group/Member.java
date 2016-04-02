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
 * limitations under the License.
 */
package io.atomix.group;

import io.atomix.group.messaging.MessageClient;

/**
 * A {@link DistributedGroup} member representing a member of the group controlled by a local
 * or remote process.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Member {

  /**
   * Returns the member ID.
   * <p>
   * The member ID is guaranteed to be unique across the cluster. Depending on how the member was
   * constructed, it may be a user-provided identifier or an automatically generated {@link java.util.UUID}.
   *
   * @return The member ID.
   */
  String id();

  /**
   * Returns the member message service.
   *
   * @return The member message service.
   */
  MessageClient messages();

}
