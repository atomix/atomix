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

import io.atomix.messaging.Endpoint;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a controller instance as a member in a cluster.
 */
public class Node {

  /**
   * Returns a new node builder with no ID.
   *
   * @return the node builder
   */
  public static Builder builder() {
    return new Builder(null);
  }

  /**
   * Returns a new node builder.
   *
   * @param nodeId the node identifier
   * @return the node builder
   * @throws NullPointerException if the node ID is null
   */
  public static Builder builder(String nodeId) {
    return builder(NodeId.from(nodeId));
  }

  /**
   * Returns a new node builder.
   *
   * @param nodeId the node identifier
   * @return the node builder
   * @throws NullPointerException if the node ID is null
   */
  public static Builder builder(NodeId nodeId) {
    return new Builder(checkNotNull(nodeId, "nodeId cannot be null"));
  }

  /**
   * Returns a new data node.
   *
   * @param nodeId   the data node ID
   * @param endpoint the data node endpoint
   * @return a new data node
   */
  public static Node data(NodeId nodeId, Endpoint endpoint) {
    return builder(nodeId)
        .withType(Type.DATA)
        .withEndpoint(endpoint)
        .build();
  }

  /**
   * Returns a new client node.
   *
   * @param nodeId   the client node ID
   * @param endpoint the client node endpoint
   * @return a new client node
   */
  public static Node client(NodeId nodeId, Endpoint endpoint) {
    return builder(nodeId)
        .withType(Type.CLIENT)
        .withEndpoint(endpoint)
        .build();
  }

  /**
   * Node type.
   */
  public enum Type {

    /**
     * Represents a data node.
     */
    DATA,

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
  private final Type type;
  private final Endpoint endpoint;

  protected Node(NodeId id, Type type, Endpoint endpoint) {
    this.id = checkNotNull(id, "id cannot be null");
    this.type = checkNotNull(type, "type cannot be null");
    this.endpoint = checkNotNull(endpoint, "endpoint cannot be null");
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
   * Returns the node type.
   *
   * @return the node type
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the node endpoint.
   *
   * @return the node endpoint
   */
  public Endpoint endpoint() {
    return endpoint;
  }

  /**
   * Returns the node state.
   *
   * @return the node state
   */
  public State getState() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Node && ((Node) object).id.equals(id);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("type", type)
        .add("endpoint", endpoint)
        .toString();
  }

  /**
   * Node builder.
   */
  public static class Builder implements io.atomix.utils.Builder<Node> {
    protected NodeId id;
    protected Type type;
    protected Endpoint endpoint;

    protected Builder(NodeId id) {
      this.id = id;
    }

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
     * Sets the node type.
     *
     * @param type the node type
     * @return the node builder
     * @throws NullPointerException if the node type is null
     */
    public Builder withType(Type type) {
      this.type = checkNotNull(type, "type cannot be null");
      return this;
    }

    /**
     * Sets the node endpoint.
     *
     * @param endpoint the node endpoint
     * @return the node builder
     */
    public Builder withEndpoint(Endpoint endpoint) {
      this.endpoint = checkNotNull(endpoint, "endpoint cannot be null");
      return this;
    }

    @Override
    public Node build() {
      if (id == null) {
        id = NodeId.from(endpoint.host().getHostName());
      }
      return new Node(id, type, endpoint);
    }
  }
}
