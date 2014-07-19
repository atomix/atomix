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
package net.kuujo.copycat.log;

import java.util.Set;

/**
 * A special configuration log entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConfigurationEntry extends Entry {
  private Set<String> members;

  public ConfigurationEntry() {
    super();
  }

  public ConfigurationEntry(long term, Set<String> members) {
    super(term);
    this.members = members;
  }

  /**
   * Returns a set of updated cluster members.
   * 
   * @return A set of cluster member addresses.
   */
  public Set<String> members() {
    return members;
  }

  @Override
  public String toString() {
    return String.format("Configuration%s", members);
  }

}
