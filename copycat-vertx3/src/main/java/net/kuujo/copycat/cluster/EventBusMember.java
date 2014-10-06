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
 * Event bus member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusMember extends Member {
  private final String address;

  public EventBusMember(String address) {
    super(address);
    this.address = address;
  }

  public EventBusMember(EventBusMemberConfig config) {
    super(config.getId());
    this.address = config.getAddress();
  }

  /**
   * Returns the event bus member address.
   *
   * @return The member's event bus address.
   */
  public String address() {
    return address;
  }

  @Override
  public String toString() {
    return String.format("%s[address=%s]", getClass().getSimpleName(), address);
  }

}
