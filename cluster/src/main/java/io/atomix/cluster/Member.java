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

import com.google.common.collect.ImmutableSet;
import io.atomix.utils.config.Configured;
import io.atomix.utils.net.Address;

import java.util.Objects;
import java.util.Set;

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
    return new Builder(checkNotNull(memberId, "memberId cannot be null"));
  }

  /**
   * Returns a new persistent node.
   *
   * @param memberId  the persistent node ID
   * @param address the persistent node address
   * @return a new persistent node
   */
  public static Member persistent(MemberId memberId, Address address) {
    return builder(memberId)
        .withType(Type.PERSISTENT)
        .withAddress(address)
        .build();
  }

  /**
   * Returns a new ephemeral node.
   *
   * @param memberId  the ephemeral node ID
   * @param address the ephemeral node address
   * @return a new ephemeral node
   */
  public static Member ephemeral(MemberId memberId, Address address) {
    return builder(memberId)
        .withType(Type.EPHEMERAL)
        .withAddress(address)
        .build();
  }

  /**
   * Node type.
   */
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
  private final Type type;
  private final Address address;
  private final String zone;
  private final String rack;
  private final String host;
  private final Set<String> tags;

  public Member(MemberConfig config) {
    this.id = checkNotNull(config.getId(), "id cannot be null");
    this.type = checkNotNull(config.getType(), "type cannot be null");
    this.address = checkNotNull(config.getAddress(), "address cannot be null");
    this.zone = config.getZone();
    this.rack = config.getRack();
    this.host = config.getHost();
    this.tags = ImmutableSet.copyOf(config.getTags());
  }

  protected Member(MemberId id, Type type, Address address, String zone, String rack, String host, Set<String> tags) {
    this.id = checkNotNull(id, "id cannot be null");
    this.type = checkNotNull(type, "type cannot be null");
    this.address = checkNotNull(address, "address cannot be null");
    this.zone = zone;
    this.rack = rack;
    this.host = host;
    this.tags = ImmutableSet.copyOf(tags);
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

  /**
   * Returns the node tags.
   *
   * @return the node tags
   */
  public Set<String> tags() {
    return tags;
  }

  @Override
  public MemberConfig config() {
    return new MemberConfig()
        .setId(id)
        .setType(type)
        .setAddress(address)
        .setZone(zone)
        .setRack(rack)
        .setHost(host)
        .setTags(tags);
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
  public static class Builder implements io.atomix.utils.Builder<Member> {
    protected final MemberConfig config = new MemberConfig();

    protected Builder(MemberId id) {
      config.setId(id);
    }

    /**
     * Sets the node identifier.
     *
     * @param id the node identifier
     * @return the node builder
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
    public Builder withType(Type type) {
      config.setType(type);
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
      config.setAddress(address);
      return this;
    }

    /**
     * Sets the zone to which the node belongs.
     *
     * @param zone the zone to which the node belongs
     * @return the node builder
     */
    public Builder withZone(String zone) {
      config.setZone(zone);
      return this;
    }

    /**
     * Sets the rack to which the node belongs.
     *
     * @param rack the rack to which the node belongs
     * @return the node builder
     */
    public Builder withRack(String rack) {
      config.setRack(rack);
      return this;
    }

    /**
     * Sets the host to which the node belongs.
     *
     * @param host the host to which the node belongs
     * @return the node builder
     */
    public Builder withHost(String host) {
      config.setHost(host);
      return this;
    }

    /**
     * Sets the node tags.
     *
     * @param tags the node tags
     * @return the node builder
     * @throws NullPointerException if the tags are null
     */
    public Builder withTags(Set<String> tags) {
      config.setTags(tags);
      return this;
    }

    /**
     * Adds a tag to the node.
     *
     * @param tag the tag to add
     * @return the node builder
     * @throws NullPointerException if the tag is null
     */
    public Builder addTag(String tag) {
      config.addTag(tag);
      return this;
    }

    @Override
    public Member build() {
      return new Member(config);
    }
  }
}
