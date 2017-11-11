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
import io.atomix.messaging.Endpoint;

import java.util.Objects;

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
  public static Builder builder() {
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
  private final Endpoint endpoint;

  protected Node(NodeId id, Endpoint endpoint) {
    this.id = checkNotNull(id, "id cannot be null");
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
   * Returns the node endpoint.
   *
   * @return the node endpoint
   */
  public Endpoint endpoint() {
    return endpoint;
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
        .add("endpoint", endpoint)
        .toString();
  }

  /**
   * Node builder.
   */
  public abstract static class Builder implements io.atomix.utils.Builder<Node> {
    protected NodeId id;
    protected Endpoint endpoint;

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
     * Sets the node endpoint.
     *
     * @param endpoint the node endpoint
     * @return the node builder
     */
    public Builder withEndpoint(Endpoint endpoint) {
      this.endpoint = checkNotNull(endpoint, "endpoint cannot be null");
      return this;
    }
  }
}
