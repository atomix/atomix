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

import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Default cluster node.
 */
public class DefaultNode extends Node {
  private State state = State.INACTIVE;

  public DefaultNode(NodeId id, InetAddress address, int port) {
    super(id, address, port);
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
  public State state() {
    return state;
  }

  /**
   * Default cluster node builder.
   */
  public static class Builder extends Node.Builder {
    @Override
    public Node build() {
      if (address == null) {
        try {
          address = InetAddress.getByName("127.0.0.1");
        } catch (UnknownHostException e) {
          throw new IllegalStateException("Failed to instantiate address", e);
        }
      }
      return new DefaultNode(id, address, port);
    }
  }
}
