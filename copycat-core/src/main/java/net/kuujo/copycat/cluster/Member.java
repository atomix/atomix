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
 * Cluster member.<p>
 *
 * Each Copycat cluster is made up of any number of @{code Member} nodes. The {@code Member} is essentially an immutable
 * single-node configuration based on a mutable {@link net.kuujo.copycat.cluster.MemberConfig}. Members and their
 * configuration implementations can differ based on the protocol which they support. For instance, a {@code TcpMember}
 * might provide a {@code host} and {@code port} as required by the TCP protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Member {
  private final String id;

  public Member() {
    id = null;
  }

  public Member(String id) {
    this.id = id;
  }

  public Member(MemberConfig config) {
    this(config.getId());
  }

  /**
   * Returns the unique member ID.
   *
   * @return The unique member ID.
   */
  public String id() {
    return id;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Member && ((Member) object).id.equals(id);
  }

  @Override
  public int hashCode() {
    int hashCode = 47;
    hashCode = 37 * hashCode + id.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), id);
  }

}
