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

  private final Collection<Node> nodes;

  public ClusterMetadata(Collection<Node> nodes) {
    this.nodes = nodes;
  }

  /**
   * Returns the collection of bootstrap nodes.
   *
   * @return the collection of bootstrap nodes
   */
  public Collection<Node> nodes() {
    return nodes;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("nodes", nodes)
        .toString();
  }

  /**
   * Cluster metadata builder.
   */
  public static class Builder implements io.atomix.utils.Builder<ClusterMetadata> {
    protected Collection<Node> nodes;

    /**
     * Sets the bootstrap nodes.
     *
     * @param bootstrapNodes the nodes from which to bootstrap the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the bootstrap nodes are {@code null}
     */
    public Builder withNodes(Node... bootstrapNodes) {
      return withNodes(Arrays.asList(checkNotNull(bootstrapNodes)));
    }

    /**
     * Sets the nodes.
     *
     * @param nodes the nodes from which to the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the nodes are {@code null}
     */
    public Builder withNodes(Collection<Node> nodes) {
      this.nodes = checkNotNull(nodes, "nodes cannot be null");
      return this;
    }

    @Override
    public ClusterMetadata build() {
      return new ClusterMetadata(nodes);
    }
  }
}
