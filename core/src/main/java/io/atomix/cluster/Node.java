/*
 * Copyright 2014-present Open Networking Foundation
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

import java.net.InetAddress;

/**
 * Represents a controller instance as a member in a cluster.
 */
public interface Node {

  /**
   * Represents the operational state of the instance.
   */
  enum State {

    /**
     * Signifies that the instance is active and that all components are
     * operating normally.
     */
    READY,

    /**
     * Signifies that the instance is active and operating normally.
     */
    ACTIVE,

    /**
     * Signifies that the instance is inactive, which means either down or
     * up, but not operational.
     */
    INACTIVE;

    /**
     * Indicates whether the state represents node which is active or ready.
     *
     * @return true if active or ready
     */
    public boolean isActive() {
      return this == ACTIVE || this == READY;
    }

    /**
     * Indicates whether the state represents a node which is ready.
     *
     * @return true if active and ready
     */
    public boolean isReady() {
      return this == READY;
    }
  }

  /**
   * Returns the instance identifier.
   *
   * @return instance identifier
   */
  NodeId id();

  /**
   * Returns the IP address of the controller instance.
   *
   * @return IP address
   */
  InetAddress getAddress();

  /**
   * Returns the TCP port on which the node listens for connections.
   *
   * @return TCP port
   */
  int getPort();

}
