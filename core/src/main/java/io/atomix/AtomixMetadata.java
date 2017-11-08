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
package io.atomix;

import com.google.common.collect.Sets;
import io.atomix.cluster.Node;
import io.atomix.cluster.Node.Type;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.impl.DefaultNode;
import io.atomix.partition.PartitionId;
import io.atomix.partition.PartitionMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster metadata.
 */
public class AtomixMetadata {

  /**
   * Returns a new cluster metadata builder.
   *
   * @return a new cluster metadata builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  private final String name;
  private final Node localNode;
  private final Collection<Node> bootstrapNodes;
  private final Collection<PartitionMetadata> partitions;
  private final int buckets;

  protected AtomixMetadata(
      String name,
      Node localNode,
      Collection<Node> bootstrapNodes,
      Collection<PartitionMetadata> partitions,
      int buckets) {
    this.name = checkNotNull(name, "name cannot be null");
    this.localNode = ((DefaultNode) localNode).setType(bootstrapNodes.contains(localNode) ? Type.CORE : Type.CLIENT);
    this.bootstrapNodes = bootstrapNodes.stream()
        .map(node -> ((DefaultNode) node).setType(Type.CORE))
        .collect(Collectors.toList());
    this.partitions = checkNotNull(partitions, "partitions cannot be null");
    this.buckets = buckets;
  }

  /**
   * Returns the cluster name.
   *
   * @return the cluster name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the local node.
   *
   * @return the local node
   */
  public Node localNode() {
    return localNode;
  }

  /**
   * Returns the collection of bootstrap nodes.
   *
   * @return the collection of bootstrap nodes
   */
  public Collection<Node> bootstrapNodes() {
    return bootstrapNodes;
  }

  /**
   * Returns the collection of partitions.
   *
   * @return the collection of partitions.
   */
  public Collection<PartitionMetadata> partitions() {
    return partitions;
  }

  /**
   * Returns the number of buckets.
   *
   * @return the number of buckets
   */
  public int buckets() {
    return buckets;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("localNode", localNode)
        .add("bootstrapNodes", bootstrapNodes)
        .add("partitions", partitions)
        .toString();
  }

  /**
   * Cluster metadata builder.
   */
  public static class Builder implements io.atomix.utils.Builder<AtomixMetadata> {
    private static final String DEFAULT_CLUSTER_NAME = "atomix";
    protected String name = DEFAULT_CLUSTER_NAME;
    protected Node localNode;
    protected Collection<Node> bootstrapNodes;
    protected int numPartitions;
    protected int partitionSize;
    protected int numBuckets;
    protected Collection<PartitionMetadata> partitions;

    /**
     * Sets the cluster name.
     *
     * @param name the cluster name
     * @return the cluster metadata builder
     * @throws NullPointerException if the name is null
     */
    public Builder withClusterName(String name) {
      this.name = checkNotNull(name, "name cannot be null");
      return this;
    }

    /**
     * Sets the local node metadata.
     *
     * @param localNode the local node metadata
     * @return the cluster metadata builder
     */
    public Builder withLocalNode(Node localNode) {
      this.localNode = checkNotNull(localNode, "localNode cannot be null");
      return this;
    }

    /**
     * Sets the bootstrap nodes.
     *
     * @param bootstrapNodes the nodes from which to bootstrap the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the bootstrap nodes are {@code null}
     */
    public Builder withBootstrapNodes(Node... bootstrapNodes) {
      return withBootstrapNodes(Arrays.asList(checkNotNull(bootstrapNodes)));
    }

    /**
     * Sets the bootstrap nodes.
     *
     * @param bootstrapNodes the nodes from which to bootstrap the cluster
     * @return the cluster metadata builder
     * @throws NullPointerException if the bootstrap nodes are {@code null}
     */
    public Builder withBootstrapNodes(Collection<Node> bootstrapNodes) {
      this.bootstrapNodes = checkNotNull(bootstrapNodes, "bootstrapNodes cannot be null");
      return this;
    }

    /**
     * Sets the number of partitions.
     *
     * @param numPartitions the number of partitions
     * @return the cluster metadata builder
     * @throws IllegalArgumentException if the number of partitions is not positive
     */
    public Builder withNumPartitions(int numPartitions) {
      checkArgument(numPartitions > 0, "numPartitions must be positive");
      this.numPartitions = numPartitions;
      return this;
    }

    /**
     * Sets the partition size.
     *
     * @param partitionSize the partition size
     * @return the cluster metadata builder
     * @throws IllegalArgumentException if the partition size is not positive
     */
    public Builder withPartitionSize(int partitionSize) {
      checkArgument(partitionSize > 0, "partitionSize must be positive");
      this.partitionSize = partitionSize;
      return this;
    }

    /**
     * Sets the number of buckets within each partition.
     *
     * @param numBuckets the number of buckets within each partition
     * @return the cluster metadata builder
     * @throws IllegalArgumentException if the number of buckets within each partition is not positive
     */
    public Builder withNumBuckets(int numBuckets) {
      checkArgument(numBuckets > 0, "numBuckets must be positive");
      this.numBuckets = numBuckets;
      return this;
    }

    /**
     * Sets the partitions.
     *
     * @param partitions the partitions
     * @return the cluster metadata builder
     */
    public Builder withPartitions(Collection<PartitionMetadata> partitions) {
      this.partitions = partitions;
      return this;
    }

    /**
     * Builds the cluster partitions.
     */
    private Collection<PartitionMetadata> buildPartitions() {
      if (partitions != null) {
        return partitions;
      }

      List<Node> sorted = new ArrayList<>(bootstrapNodes.size());
      sorted.sort(Comparator.comparing(Node::id));

      Set<PartitionMetadata> partitions = Sets.newHashSet();
      for (int i = 0; i < numPartitions; i++) {
        Set<NodeId> set = new HashSet<>(partitionSize);
        for (int j = 0; j < partitionSize; j++) {
          set.add(sorted.get((i + j) % numPartitions).id());
        }
        partitions.add(new PartitionMetadata(PartitionId.from((i + 1)), set));
      }
      return partitions;
    }

    @Override
    public AtomixMetadata build() {
      return new AtomixMetadata(
          name,
          localNode,
          bootstrapNodes,
          buildPartitions(),
          numBuckets);
    }
  }
}
