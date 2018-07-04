/*
 * Copyright 2018-present Open Networking Foundation
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

import java.util.HashMap;
import java.util.Map;

/**
 * Member configuration.
 */
public class MemberConfig extends NodeConfig {
  private MemberId id = MemberId.anonymous();
  private String zone;
  private String rack;
  private String host;
  private Map<String, String> metadata = new HashMap<>();

  /**
   * Returns the member identifier.
   *
   * @return the member identifier
   */
  public MemberId getId() {
    return id;
  }

  /**
   * Sets the member identifier.
   *
   * @param id the member identifier
   * @return the member configuration
   */
  public MemberConfig setId(String id) {
    return setId(id != null ? MemberId.from(id) : null);
  }

  @Override
  public MemberConfig setId(NodeId id) {
    return setId(id != null ? MemberId.from(id.id()) : null);
  }

  /**
   * Sets the member identifier.
   *
   * @param id the member identifier
   * @return the member configuration
   */
  public MemberConfig setId(MemberId id) {
    this.id = id != null ? id : MemberId.anonymous();
    return this;
  }

  /**
   * Returns the member address.
   *
   * @return the member address
   */
  public Address getAddress() {
    return super.getAddress();
  }

  /**
   * Sets the member address.
   *
   * @param address the member address
   * @return the member configuration
   */
  public MemberConfig setAddress(String address) {
    super.setAddress(address);
    return this;
  }

  /**
   * Sets the member address.
   *
   * @param address the member address
   * @return the member configuration
   */
  public MemberConfig setAddress(Address address) {
    super.setAddress(address);
    return this;
  }

  /**
   * Returns the member zone.
   *
   * @return the member zone
   */
  public String getZone() {
    return zone;
  }

  /**
   * Sets the member zone.
   *
   * @param zone the member zone
   * @return the member configuration
   */
  public MemberConfig setZone(String zone) {
    this.zone = zone;
    return this;
  }

  /**
   * Returns the member rack.
   *
   * @return the member rack
   */
  public String getRack() {
    return rack;
  }

  /**
   * Sets the member rack.
   *
   * @param rack the member rack
   * @return the member configuration
   */
  public MemberConfig setRack(String rack) {
    this.rack = rack;
    return this;
  }

  /**
   * Returns the member host.
   *
   * @return the member host
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the member host.
   *
   * @param host the member host
   * @return the member configuration
   */
  public MemberConfig setHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Returns the member metadata.
   *
   * @return the member metadata
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * Sets the member metadata.
   *
   * @param metadata the member metadata
   * @return the member configuration
   */
  public MemberConfig setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
    return this;
  }

  /**
   * Adds member metadata.
   *
   * @param key   the metadata key to add
   * @param value the metadata value to add
   * @return the member configuration
   */
  public MemberConfig addMetadata(String key, String value) {
    this.metadata.put(key, value);
    return this;
  }
}
