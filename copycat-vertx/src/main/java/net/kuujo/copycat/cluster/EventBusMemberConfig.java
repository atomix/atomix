/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.cluster;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusMemberConfig extends MemberConfig {

  public EventBusMemberConfig() {
  }

  public EventBusMemberConfig(String address) {
    super(address);
  }

  /**
   * Sets the member event bus address.
   *
   * @param address The member event bus address.
   */
  public void setAddress(String address) {
    setId(address);
  }

  /**
   * Returns the member event bus address.
   *
   * @return The member event bus address.
   */
  public String getAddress() {
    return getId();
  }

  /**
   * Sets the member event bus address, returning the configuration for method chaining.
   *
   * @param address The member event bus address.
   * @return The member configuration.
   */
  public EventBusMemberConfig withAddress(String address) {
    setId(address);
    return this;
  }

}
