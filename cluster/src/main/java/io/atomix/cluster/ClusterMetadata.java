/*
 * Copyright 2017-present Open Networking Foundation
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

import java.util.Arrays;
import java.util.Collection;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster metadata.
 */
public class ClusterMetadata {

  /**
   * Returns a new cluster metadata builder.
   *
   * @return a new cluster metadata builder
   */
  public static Builder builder() {
    return new Builder();
  }

  private final Collection<Member> members;

  public ClusterMetadata(Collection<Member> members) {
    this.members = members;
  }

  /**
   * Returns the collection of members.
   *
   * @return the collection of members
   */
  public Collection<Member> members() {
    return members;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("members", members)
        .toString();
  }

  /**
   * Cluster metadata builder.
   */
  public static class Builder implements io.atomix.utils.Builder<ClusterMetadata> {
    protected Collection<Member> members;

    /**
     * Sets the bootstrap nodes.
     *
     * @param bootstrapMembers the nodes from which to bootstrap the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the bootstrap nodes are {@code null}
     */
    public Builder withNodes(Member... bootstrapMembers) {
      return withNodes(Arrays.asList(checkNotNull(bootstrapMembers)));
    }

    /**
     * Sets the nodes.
     *
     * @param members the nodes from which to the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the nodes are {@code null}
     */
    public Builder withNodes(Collection<Member> members) {
      this.members = checkNotNull(members, "nodes cannot be null");
      return this;
    }

    @Override
    public ClusterMetadata build() {
      return new ClusterMetadata(members);
    }
  }
}
