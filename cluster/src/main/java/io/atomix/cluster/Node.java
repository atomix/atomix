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

import io.atomix.utils.net.Address;

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
   * Returns a new core node.
   *
   * @param nodeId   the core node ID
   * @param address the core node address
   * @return a new core node
   */
  public static Node core(NodeId nodeId, Address address) {
    return builder(nodeId)
        .withType(Type.CORE)
        .withAddress(address)
        .build();
  }

  /**
   * Returns a new data node.
   *
   * @param nodeId   the data node ID
   * @param address the data node address
   * @return a new data node
   */
  public static Node data(NodeId nodeId, Address address) {
    return builder(nodeId)
        .withType(Type.DATA)
        .withAddress(address)
        .build();
  }

  /**
   * Returns a new client node.
   *
   * @param nodeId   the client node ID
   * @param address the client node address
   * @return a new client node
   */
  public static Node client(NodeId nodeId, Address address) {
    return builder(nodeId)
        .withType(Type.CLIENT)
        .withAddress(address)
        .build();
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
  private final Address address;
  private final String zone;
  private final String rack;
  private final String host;

  public Node(NodeConfig config) {
    this(config.getId(), config.getType(), config.getAddress(), config.getZone(), config.getRack(), config.getHost());
  }

  protected Node(NodeId id, Type type, Address address, String zone, String rack, String host) {
    this.id = checkNotNull(id, "id cannot be null");
    this.type = checkNotNull(type, "type cannot be null");
    this.address = checkNotNull(address, "address cannot be null");
    this.zone = zone;
    this.rack = rack;
    this.host = host;
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
   * Returns the node address.
   *
   * @return the node address
   */
  public Address address() {
    return address;
  }

  /**
   * Returns the node state.
   *
   * @return the node state
   */
  public State getState() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the zone to which the node belongs.
   *
   * @return the zone to which the node belongs
   */
  public String zone() {
    return zone;
  }

  /**
   * Returns the rack to which the node belongs.
   *
   * @return the rack to which the node belongs
   */
  public String rack() {
    return rack;
  }

  /**
   * Returns the host to which the rack belongs.
   *
   * @return the host to which the rack belongs
   */
  public String host() {
    return host;
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
        .add("address", address)
        .add("zone", zone)
        .add("rack", rack)
        .add("host", host)
        .omitNullValues()
        .toString();
  }

  /**
   * Node builder.
   */
  public static class Builder implements io.atomix.utils.Builder<Node> {
    protected NodeId id;
    protected Type type;
    protected Address address;
    protected String zone;
    protected String rack;
    protected String host;

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
     * Sets the node address.
     *
     * @param address a host:port tuple
     * @return the node builder
     * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
     */
    public Builder withAddress(String address) {
      return withAddress(Address.from(address));
    }

    /**
     * Sets the node host/port.
     *
     * @param host the host name
     * @param port the port number
     * @return the node builder
     * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
     */
    public Builder withAddress(String host, int port) {
      return withAddress(Address.from(host, port));
    }

    /**
     * Sets the node address using local host.
     *
     * @param port the port number
     * @return the node builder
     * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
     */
    public Builder withAddress(int port) {
      return withAddress(Address.from(port));
    }

    /**
     * Sets the node address.
     *
     * @param address the node address
     * @return the node builder
     */
    public Builder withAddress(Address address) {
      this.address = checkNotNull(address, "address cannot be null");
      return this;
    }

    /**
     * Sets the zone to which the node belongs.
     *
     * @param zone the zone to which the node belongs
     * @return the node builder
     */
    public Builder withZone(String zone) {
      this.zone = zone;
      return this;
    }

    /**
     * Sets the rack to which the node belongs.
     *
     * @param rack the rack to which the node belongs
     * @return the node builder
     */
    public Builder withRack(String rack) {
      this.rack = rack;
      return this;
    }

    /**
     * Sets the host to which the node belongs.
     *
     * @param host the host to which the node belongs
     * @return the node builder
     */
    public Builder withHost(String host) {
      this.host = host;
      return this;
    }

    @Override
    public Node build() {
      if (id == null) {
        id = NodeId.from(address.address().getHostName());
      }
      return new Node(id, type, address, zone, rack, host);
    }
  }
}
