/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.cluster;

/**
 * Cluster member configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemberConfig {
  private int id;
  private String address;

  public MemberConfig() {
  }

  public MemberConfig(int id) {
    if (id <= 0)
      throw new IllegalArgumentException("id cannot be negative");
    this.id = id;
  }

  public MemberConfig(int id, String address) {
    this.id = id;
    this.address = address;
  }

  /**
   * Sets the unique member identifier.
   *
   * @param id The unique member identifier.
   * @throws java.lang.NullPointerException If the member identifier is {@code null}
   */
  public void setId(int id) {
    if (id <= 0)
      throw new IllegalArgumentException("id cannot be negative");
    this.id = id;
  }

  /**
   * Returns the unique member identifier.
   *
   * @return The unique member identifier.
   */
  public int getId() {
    return id;
  }

  /**
   * Sets the unique member identifier, returning the member configuration for method chaining.
   *
   * @param id The unique member identifier.
   * @return The member configuration.
   * @throws java.lang.NullPointerException If the member identifier is {@code null}
   */
  public MemberConfig withId(int id) {
    setId(id);
    return this;
  }

  /**
   * Sets the unique member address.
   *
   * @param address The unique member address.
   * @throws java.lang.NullPointerException If the member address is {@code null}
   */
  public void setAddress(String address) {
    if (address == null)
      throw new NullPointerException("address cannot be null");
    this.address = address;
  }

  /**
   * Returns the unique member address.
   *
   * @return The unique member address.
   */
  public String getAddress() {
    return address;
  }

  /**
   * Sets the unique member address, returning the configuration for method chaining.
   *
   * @param address The unique member address.
   * @return The member configuration.
   * @throws java.lang.NullPointerException If the member address is {@code null}
   */
  public MemberConfig withAddress(String address) {
    setAddress(address);
    return this;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof MemberConfig) {
      MemberConfig config = (MemberConfig) object;
      return config.id == id && ((config.address == null && address == null) || (config.address != null && address != null && config.address.equals(address)));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id;
    hashCode = 37 * hashCode + (address != null ? address.hashCode() : 0);
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("MemberConfig[id=%s, address=%s]", id, address);
  }

}
