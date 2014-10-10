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

import net.kuujo.copycat.internal.util.Assert;

/**
 * Cluster member configuration.
 * <p>
 *
 * Member configurations are the mutable configurations that underly Copycat cluster members. In
 * most cases, each cluster {@link net.kuujo.copycat.cluster.Member} type will have an associated
 * mutable {@code MemberConfig}. Each member configuration must provide a unique identifier.
 * However, in many cases construction of the unique identifier may be handled by a specific
 * {@code MemberConfig} implementation. For instance, the {@code TcpMemberConfig} constructs the
 * unique {@code id} from a combination of {@code host} and {@code port}. Consult the configuration
 * documentation for specific requirements of each member configuration type.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MemberConfig {
  private String id;

  public MemberConfig() {}

  public MemberConfig(String id) {
    this.id = id;
  }

  /**
   * Sets the member's unique ID.
   *
   * @param id The member's unique ID.
   * @throws NullPointerException if {@code id} is null
   */
  public void setId(String id) {
    this.id = Assert.isNotNull(id, "id");
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
   * @throws NullPointerException if {@code id} is null
   */
  public MemberConfig withId(String id) {
    this.id = Assert.isNotNull(id, "id");
    return this;
  }

  @Override
  public boolean equals(Object object) {
    return getClass().isInstance(object) && ((MemberConfig) object).id.equals(id);
  }

  @Override
  public int hashCode() {
    int hashCode = 41;
    hashCode = 37 * hashCode + id.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), id);
  }

}
