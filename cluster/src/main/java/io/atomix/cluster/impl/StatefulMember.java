/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.cluster.impl;

import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.utils.net.Address;

import java.util.Map;

/**
 * Default cluster node.
 */
public class StatefulMember extends Member {
  private State state = State.INACTIVE;

  public StatefulMember(
      MemberId id,
      Address address,
      String zone,
      String rack,
      String host,
      Map<String, String> metadata) {
    super(id, address, zone, rack, host, metadata);
  }

  /**
   * Updates the node state.
   *
   * @param state the node state
   */
  void setState(State state) {
    this.state = state;
  }

  @Override
  public State getState() {
    return state;
  }
}
