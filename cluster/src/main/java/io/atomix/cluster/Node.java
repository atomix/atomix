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

import io.atomix.cluster.impl.DefaultNode;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a controller instance as a member in a cluster.
 */
public abstract class Node {

  /**
   * Returns a new node builder.
   *
   * @return a new node builder
   */
  public static Builder newBuilder() {
    return new DefaultNode.Builder();
  }

  /**
   * Node type.
   */
  public enum Type {

    /**
     * Represents a core node.
     */
    CORE,

    /**
     * Represents a client node.
     */
    CLIENT,
  }

  /**
   * Represents the operational state of the instance.
   */
  public enum State {

    /**
     * Signifies that the instance is active and operating normally.
     */
    ACTIVE,

    /**
     * Signifies that the instance is inactive, which means either down or
     * up, but not operational.
     */
    INACTIVE,
  }

  private final NodeId id;
  private final InetAddress address;
  private final int port;

  protected Node(NodeId id, InetAddress address, int port) {
    this.id = checkNotNull(id, "id cannot be null");
    this.address = checkNotNull(address, "address cannot be null");
    this.port = port;
  }

  /**
   * Returns the instance identifier.
   *
   * @return instance identifier
   */
  public NodeId id() {
    return id;
  }

  /**
   * Returns the IP address of the controller instance.
   *
   * @return IP address
   */
  public InetAddress address() {
    return address;
  }

  /**
   * Returns the TCP port on which the node listens for connections.
   *
   * @return TCP port
   */
  public int port() {
    return port;
  }

  /**
   * Returns the node type.
   *
   * @return the node type
   */
  public abstract Type type();

  /**
   * Returns the node state.
   *
   * @return the node state
   */
  public abstract State state();

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("address", address)
        .add("port", port)
        .toString();
  }

  /**
   * Node builder.
   */
  public abstract static class Builder implements io.atomix.utils.Builder<Node> {
    private static final int DEFAULT_PORT = 5678;

    protected NodeId id;
    protected InetAddress address;
    protected int port = DEFAULT_PORT;

    /**
     * Sets the node identifier.
     *
     * @param id the node identifier
     * @return the node builder
     */
    public Builder withId(NodeId id) {
      this.id = checkNotNull(id, "id cannot be null");
      return this;
    }

    /**
     * Sets the node host.
     *
     * @param host the node host
     * @return the node builder
     * @throws IllegalArgumentException if the host name cannot be resolved
     */
    public Builder withHost(String host) {
      try {
        return withAddress(InetAddress.getByName(host));
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Failed to resolve host", e);
      }
    }

    /**
     * Sets the node address.
     *
     * @param address the node address
     * @return the node builder
     */
    public Builder withAddress(InetAddress address) {
      this.address = checkNotNull(address, "address cannot be null");
      return this;
    }

    /**
     * Sets the node port.
     *
     * @param port the node port
     * @return the node builder
     */
    public Builder withPort(int port) {
      this.port = port;
      return this;
    }
  }
}
