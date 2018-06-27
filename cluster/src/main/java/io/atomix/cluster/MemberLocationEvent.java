/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster;

import io.atomix.utils.event.AbstractEvent;
import io.atomix.utils.net.Address;

/**
 * Member location event.
 */
public class MemberLocationEvent extends AbstractEvent<MemberLocationEvent.Type, Address> {

  /**
   * Location event type.
   */
  public enum Type {

    /**
     * Called when a member has joined the cluster.
     */
    JOIN,

    /**
     * Called when a member has left the cluster.
     */
    LEAVE,
  }

  public MemberLocationEvent(Type type, Address subject) {
    super(type, subject);
  }

  public MemberLocationEvent(Type type, Address subject, long time) {
    super(type, subject, time);
  }

  /**
   * Returns the member location address.
   *
   * @return the member location address
   */
  public Address address() {
    return subject();
  }
}
