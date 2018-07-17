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
import java.util.Properties;

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
  public static MemberBuilder builder() {
    return new MemberBuilder(new MemberConfig());
  }

  /**
   * Returns a new member builder.
   *
   * @param memberId the member identifier
   * @return the member builder
   * @throws NullPointerException if the member ID is null
   */
  public static MemberBuilder builder(String memberId) {
    return builder(MemberId.from(memberId));
  }

  /**
   * Returns a new member builder.
   *
   * @param memberId the member identifier
   * @return the member builder
   * @throws NullPointerException if the member ID is null
   */
  public static MemberBuilder builder(MemberId memberId) {
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
  private final Properties properties;

  public Member(MemberConfig config) {
    super(config);
    this.id = config.getId();
    this.zone = config.getZone();
    this.rack = config.getRack();
    this.host = config.getHost();
    this.properties = new Properties();
    properties.putAll(config.getProperties());
  }

  protected Member(MemberId id, Address address) {
    this(id, address, null, null, null, new Properties());
  }

  protected Member(MemberId id, Address address, String zone, String rack, String host, Properties properties) {
    super(id, address);
    this.id = checkNotNull(id, "id cannot be null");
    this.zone = zone;
    this.rack = rack;
    this.host = host;
    this.properties = new Properties();
    this.properties.putAll(properties);
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
   * Returns the zone to which the member belongs.
   *
   * @return the zone to which the member belongs
   */
  public String zone() {
    return zone;
  }

  /**
   * Returns the rack to which the member belongs.
   *
   * @return the rack to which the member belongs
   */
  public String rack() {
    return rack;
  }

  /**
   * Returns the host to which the member belongs.
   *
   * @return the host to which the member belongs
   */
  public String host() {
    return host;
  }

  /**
   * Returns the member properties.
   *
   * @return the member properties
   */
  public Properties properties() {
    return properties;
  }

  @Override
  public MemberConfig config() {
    return new MemberConfig()
        .setId(id())
        .setAddress(address())
        .setZone(zone())
        .setRack(rack())
        .setHost(host())
        .setProperties(properties());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id(), address(), zone(), rack(), host(), properties());
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
          && Objects.equals(member.properties(), properties());
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
        .add("properties", properties())
        .omitNullValues()
        .toString();
  }
}
