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

import io.atomix.utils.config.Config;
import io.atomix.utils.net.Address;

import java.util.HashMap;
import java.util.Map;

/**
 * Node configuration.
 */
public class MemberConfig implements Config {
  private MemberId id = MemberId.anonymous();
  private Address address;
  private String zone;
  private String rack;
  private String host;
  private Map<String, String> metadata = new HashMap<>();

  /**
   * Returns the node identifier.
   *
   * @return the node identifier
   */
  public MemberId getId() {
    if (id == null) {
      id = MemberId.from(address.address().getHostName());
    }
    return id;
  }

  /**
   * Sets the node identifier.
   *
   * @param id the node identifier
   * @return the node configuration
   */
  public MemberConfig setId(String id) {
    return setId(MemberId.from(id));
  }

  /**
   * Sets the node identifier.
   *
   * @param id the node identifier
   * @return the node configuration
   */
  public MemberConfig setId(MemberId id) {
    this.id = id != null ? id : MemberId.anonymous();
    return this;
  }

  /**
   * Returns the node address.
   *
   * @return the node address
   */
  public Address getAddress() {
    return address;
  }

  /**
   * Sets the node address.
   *
   * @param address the node address
   * @return the node configuration
   */
  public MemberConfig setAddress(String address) {
    return setAddress(Address.from(address));
  }

  /**
   * Sets the node address.
   *
   * @param address the node address
   * @return the node configuration
   */
  public MemberConfig setAddress(Address address) {
    this.address = address;
    return this;
  }

  /**
   * Returns the node zone.
   *
   * @return the node zone
   */
  public String getZone() {
    return zone;
  }

  /**
   * Sets the node zone.
   *
   * @param zone the node zone
   * @return the node configuration
   */
  public MemberConfig setZone(String zone) {
    this.zone = zone;
    return this;
  }

  /**
   * Returns the node rack.
   *
   * @return the node rack
   */
  public String getRack() {
    return rack;
  }

  /**
   * Sets the node rack.
   *
   * @param rack the node rack
   * @return the node configuration
   */
  public MemberConfig setRack(String rack) {
    this.rack = rack;
    return this;
  }

  /**
   * Returns the node host.
   *
   * @return the node host
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the node host.
   *
   * @param host the node host
   * @return the node configuration
   */
  public MemberConfig setHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Returns the node metadata.
   *
   * @return the node metadata
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * Sets the node metadata.
   *
   * @param metadata the node metadata
   * @return the node configuration
   */
  public MemberConfig setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
    return this;
  }

  /**
   * Adds a node tag.
   *
   * @param key   the metadata key to add
   * @param value the metadata value to add
   * @return the node configuration
   */
  public MemberConfig addMetadata(String key, String value) {
    this.metadata.put(key, value);
    return this;
  }
}
