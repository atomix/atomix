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

import io.atomix.utils.config.Configured;
import io.atomix.utils.net.Address;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a controller instance as a member in a cluster.
 */
public class Member implements Configured<MemberConfig> {

  /**
   * Returns a new member builder with no ID.
   *
   * @return the member builder
   */
  public static Builder builder() {
    return new Builder(null);
  }

  /**
   * Returns a new member builder.
   *
   * @param memberId the member identifier
   * @return the member builder
   * @throws NullPointerException if the member ID is null
   */
  public static Builder builder(String memberId) {
    return builder(MemberId.from(memberId));
  }

  /**
   * Returns a new member builder.
   *
   * @param memberId the member identifier
   * @return the member builder
   * @throws NullPointerException if the member ID is null
   */
  public static Builder builder(MemberId memberId) {
    return builder().withId(memberId);
  }

  /**
   * Returns a new anonymous cluster member.
   *
   * @param address the member address
   * @return the member
   */
  public static Member member(String address) {
    return member(Address.from(address));
  }

  /**
   * Returns a new named cluster member.
   *
   * @param name    the member identifier
   * @param address the member address
   * @return the member
   */
  public static Member member(String name, String address) {
    return member(MemberId.from(name), Address.from(address));
  }

  /**
   * Returns a new anonymous cluster member.
   *
   * @param address the member address
   * @return the member
   */
  public static Member member(Address address) {
    return builder()
        .withAddress(address)
        .build();
  }

  /**
   * Returns a new named cluster member.
   *
   * @param memberId the member identifier
   * @param address  the member address
   * @return the member
   */
  public static Member member(MemberId memberId, Address address) {
    return builder(memberId)
        .withAddress(address)
        .build();
  }

  /**
   * Node type.
   */
  @Deprecated
  public enum Type {

    /**
     * Represents a persistent node.
     */
    PERSISTENT,

    /**
     * Represents an ephemeral node.
     */
    EPHEMERAL,
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

  private final MemberId id;
  private final Address address;
  private final String zone;
  private final String rack;
  private final String host;
  private final Map<String, String> metadata;

  public Member(MemberConfig config) {
    this.id = config.getId();
    this.address = checkNotNull(config.getAddress(), "address cannot be null");
    this.zone = config.getZone();
    this.rack = config.getRack();
    this.host = config.getHost();
    this.metadata = new HashMap<>(config.getMetadata());
  }

  protected Member(MemberId id, Address address, String zone, String rack, String host, Map<String, String> metadata) {
    this.id = checkNotNull(id, "id cannot be null");
    this.address = checkNotNull(address, "address cannot be null");
    this.zone = zone;
    this.rack = rack;
    this.host = host;
    this.metadata = new HashMap<>(metadata);
  }

  /**
   * Returns the instance identifier.
   *
   * @return instance identifier
   */
  public MemberId id() {
    return id;
  }

  /**
   * Returns the node type.
   *
   * @return the node type
   */
  @Deprecated
  public Type type() {
    switch (id().type()) {
      case IDENTIFIED:
        return Type.PERSISTENT;
      case ANONYMOUS:
        return Type.EPHEMERAL;
      default:
        throw new AssertionError();
    }
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

  /**
   * Returns the node metadata.
   *
   * @return the node metadata
   */
  public Map<String, String> metadata() {
    return metadata;
  }

  @Override
  public MemberConfig config() {
    return new MemberConfig()
        .setId(id)
        .setAddress(address)
        .setZone(zone)
        .setRack(rack)
        .setHost(host)
        .setMetadata(metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Member && ((Member) object).id.equals(id);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("address", address)
        .add("zone", zone)
        .add("rack", rack)
        .add("host", host)
        .add("metadata", metadata)
        .omitNullValues()
        .toString();
  }

  /**
   * Member builder.
   */
  public static class Builder implements io.atomix.utils.Builder<Member> {
    protected final MemberConfig config = new MemberConfig();

    protected Builder(MemberId id) {
      if (id != null) {
        config.setId(id);
      }
    }

    /**
     * Sets the member identifier.
     *
     * @param id the member identifier
     * @return the member builder
     */
    public Builder withId(String id) {
      return withId(MemberId.from(id));
    }

    /**
     * Sets the member identifier.
     *
     * @param id the member identifier
     * @return the member builder
     */
    public Builder withId(MemberId id) {
      config.setId(id);
      return this;
    }

    /**
     * Sets the node type.
     *
     * @param type the node type
     * @return the node builder
     * @throws NullPointerException if the node type is null
     */
    @Deprecated
    public Builder withType(Type type) {
      return this;
    }

    /**
     * Sets the member address.
     *
     * @param address a host:port tuple
     * @return the member builder
     * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
     */
    public Builder withAddress(String address) {
      return withAddress(Address.from(address));
    }

    /**
     * Sets the member host/port.
     *
     * @param host the host name
     * @param port the port number
     * @return the member builder
     * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
     */
    public Builder withAddress(String host, int port) {
      return withAddress(Address.from(host, port));
    }

    /**
     * Sets the member address using local host.
     *
     * @param port the port number
     * @return the member builder
     * @throws io.atomix.utils.net.MalformedAddressException if a valid {@link Address} cannot be constructed from the arguments
     */
    public Builder withAddress(int port) {
      return withAddress(Address.from(port));
    }

    /**
     * Sets the member address.
     *
     * @param address the member address
     * @return the member builder
     */
    public Builder withAddress(Address address) {
      config.setAddress(address);
      return this;
    }

    /**
     * Sets the zone to which the member belongs.
     *
     * @param zone the zone to which the member belongs
     * @return the member builder
     */
    public Builder withZone(String zone) {
      config.setZone(zone);
      return this;
    }

    /**
     * Sets the rack to which the member belongs.
     *
     * @param rack the rack to which the member belongs
     * @return the member builder
     */
    public Builder withRack(String rack) {
      config.setRack(rack);
      return this;
    }

    /**
     * Sets the host to which the member belongs.
     *
     * @param host the host to which the member belongs
     * @return the member builder
     */
    public Builder withHost(String host) {
      config.setHost(host);
      return this;
    }

    /**
     * Sets the member metadata.
     *
     * @param metadata the member metadata
     * @return the member builder
     * @throws NullPointerException if the tags are null
     */
    public Builder withMetadata(Map<String, String> metadata) {
      config.setMetadata(metadata);
      return this;
    }

    /**
     * Adds metadata to the member.
     *
     * @param key   the metadata key to add
     * @param value the metadata value to add
     * @return the member builder
     * @throws NullPointerException if the tag is null
     */
    public Builder addMetadata(String key, String value) {
      config.addMetadata(key, value);
      return this;
    }

    @Override
    public Member build() {
      return new Member(config);
    }
  }
}
