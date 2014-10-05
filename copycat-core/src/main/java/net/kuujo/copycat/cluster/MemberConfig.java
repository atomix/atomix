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

import net.kuujo.copycat.util.Args;

import java.util.Observable;

/**
 * Base cluster member configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemberConfig extends Observable {
  private String id;

  public MemberConfig() {
  }

  public MemberConfig(String id) {
    this.id = id;
  }

  /**
   * Sets the member's unique ID.
   *
   * @param id The member's unique ID.
   */
  public void setId(String id) {
    this.id = Args.checkNotNull(id);
    notifyObservers();
  }

  /**
   * Returns the member's unique ID.
   *
   * @return The member's unique ID.
   */
  public String getId() {
    return id;
  }

  /**
   * Sets the member's unique ID, returning the member configuration for method chaining.
   *
   * @param id The member's unique ID.
   * @return The member configuration.
   */
  public MemberConfig withId(String id) {
    this.id = Args.checkNotNull(id);
    notifyObservers();
    return this;
  }

  @Override
  public boolean equals(Object object) {
    return getClass().isInstance(object) && ((MemberConfig) object).id.equals(id);
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("MemberConfig[id=%s]", id);
  }

}
