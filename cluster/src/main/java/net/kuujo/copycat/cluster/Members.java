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

import net.kuujo.copycat.io.serializer.Serializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Cluster members.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Members {

  /**
   * Returns a member by member identifier.
   *
   * @param id The unique member identifier.
   * @return The member or {@code null} if the member does not exist.
   */
  Member member(int id);

  /**
   * Returns an immutable set of all cluster members.
   *
   * @return An immutable set of all members in the cluster.
   */
  List<Member> members();

  /**
   * Returns the cluster serializer.
   *
   * @return The cluster serializer.
   */
  Serializer serializer();

  /**
   * Adds a membership listener to the cluster.<p>
   *
   * @param listener The membership event listener to add.
   * @return The cluster.
   */
  Members addListener(MembershipListener listener);

  /**
   * Removes a membership listener from the cluster.
   *
   * @param listener The membership event listener to remove.
   * @return The cluster.
   */
  Members removeListener(MembershipListener listener);

  /**
   * Members builder.
   */
  static interface Builder<T extends Builder<T, U, V>, U extends ManagedMembers, V extends ManagedMember> extends net.kuujo.copycat.Builder<U> {

    /**
     * Sets the cluster seed members.
     *
     * @param members The set of cluster seed members.
     * @return The cluster builder.
     */
    @SuppressWarnings("unchecked")
    default T withMembers(V... members) {
      if (members != null) {
        return withMembers(Arrays.asList(members));
      }
      return (T) this;
    }

    /**
     * Sets the cluster seed members.
     *
     * @param members The set of cluster seed members.
     * @return The cluster builder.
     */
    T withMembers(Collection<V> members);

    /**
     * Adds a cluster seed member.
     *
     * @param member The cluster seed member to add.
     * @return The cluster builder.
     */
    T addMember(V member);
  }

}
