/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.event;

import java.util.Set;

/**
 * Cluster membership change event.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MembershipChangeEvent implements Event {
  private final Set<String> members;

  public MembershipChangeEvent(Set<String> members) {
    this.members = members;
  }

  /**
   * Returns the changed cluster membership.
   *
   * @return The changed cluster membership.
   */
  public Set<String> members() {
    return members;
  }

}
