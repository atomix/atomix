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

import com.google.common.collect.Maps;
import io.atomix.utils.net.Address;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Represents a node as a member in a cluster.
 */
public class Member extends Node {

  /**
   * Returns a new member builder with no ID.
   *
   * @return the member builder
   */
  public static Builder builder() {
    return new Builder(new MemberConfig());
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

  private final MemberId id;
  private final String zone;
  private final String rack;
  private final String host;
  private final Map<String, String> metadata;

  public Member(MemberConfig config) {
    super(config);
    this.id = config.getId();
    this.zone = config.getZone();
    this.rack = config.getRack();
    this.host = config.getHost();
    this.metadata = new HashMap<>(config.getMetadata());
  }

  protected Member(MemberId id, Address address) {
    this(id, address, null, null, null, Maps.newConcurrentMap());
  }

  protected Member(MemberId id, Address address, String zone, String rack, String host, Map<String, String> metadata) {
    super(id, address);
    this.id = checkNotNull(id, "id cannot be null");
    this.zone = zone;
    this.rack = rack;
    this.host = host;
    this.metadata = new HashMap<>(metadata);
  }

  @Override
  public MemberId id() {
    return id;
  }

  /**
   * Returns a boolean indicating whether this member is an active member of the cluster.
   *
   * @return indicates whether this member is an active member of the cluster
   */
  public boolean isActive() {
    return false;
  }

  /**
   * Returns the node reachability.
   *
   * @return the node reachability
   */
  public boolean isReachable() {
    return false;
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
        .setId(id())
        .setAddress(address())
        .setZone(zone())
        .setRack(rack())
        .setHost(host())
        .setMetadata(metadata());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id(), address(), zone(), rack(), host(), metadata());
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Member) {
      Member member = (Member) object;
      return member.id().equals(id())
          && member.address().equals(address())
          && Objects.equals(member.zone(), zone())
          && Objects.equals(member.rack(), rack())
          && Objects.equals(member.host(), host())
          && Objects.equals(member.metadata(), metadata());
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(Member.class)
        .add("id", id())
        .add("address", address())
        .add("zone", zone())
        .add("rack", rack())
        .add("host", host())
        .add("metadata", metadata())
        .omitNullValues()
        .toString();
  }

  /**
   * Member builder.
   */
  public static class Builder extends Node.Builder {
    protected final MemberConfig config;

    protected Builder(MemberConfig config) {
      super(config);
      this.config = config;
    }

    @Override
    public Builder withId(String id) {
      super.withId(id);
      return this;
    }

    @Override
    public Builder withId(NodeId id) {
      super.withId(id);
      return this;
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
