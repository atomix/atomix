/*
 * Copyright 2018-present Open Networking Foundation
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

import io.atomix.utils.Managed;
import io.atomix.utils.net.Address;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster configuration.
 */
public class AtomixCluster implements Managed<AtomixCluster> {

  /**
   * Returns a new cluster builder.
   *
   * @return a new cluster builder
   */
  public Builder builder() {
    return new Builder();
  }

  private final String name;
  private final Node localNode;
  private final Collection<Node> nodes;
  private final boolean multicastEnabled;
  private final Address multicastAddress;

  public AtomixCluster(ClusterConfig config) {
    this(
        config.getName(),
        new Node(config.getLocalNode()),
        config.getNodes().stream().map(Node::new).collect(Collectors.toList()),
        config.isMulticastEnabled(),
        config.getMulticastAddress());
  }

  public AtomixCluster(String name, Node localNode, Collection<Node> nodes, boolean multicastEnabled, Address multicastAddress) {
    this.name = name;
    this.localNode = localNode;
    this.nodes = nodes;
    this.multicastEnabled = multicastEnabled;
    this.multicastAddress = multicastAddress;
  }

  @Override
  public CompletableFuture<AtomixCluster> start() {
    return null;
  }

  @Override
  public boolean isRunning() {
    return false;
  }

  @Override
  public CompletableFuture<Void> stop() {
    return null;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name())
        .add("localNode", localNode())
        .add("nodes", nodes())
        .add("multicastEnabled", multicastEnabled())
        .add("multicastAddress", multicastAddress())
        .toString();
  }

  /**
   * Cluster builder.
   */
  public static class Builder implements io.atomix.utils.Builder<AtomixCluster> {
    private final ClusterConfig config = new ClusterConfig();

    /**
     * Sets the cluster name.
     *
     * @param clusterName the cluster name
     * @return the cluster builder
     */
    public Builder withName(String clusterName) {
      config.setName(clusterName);
      return this;
    }

    /**
     * Sets the local node metadata.
     *
     * @param localNode the local node metadata
     * @return the cluster metadata builder
     */
    public Builder withLocalNode(Node localNode) {
      config.setLocalNode(localNode.config);
      return this;
    }

    /**
     * Sets the core nodes.
     *
     * @param coreNodes the core nodes
     * @return the Atomix builder
     */
    public Builder withNodes(Node... coreNodes) {
      return withNodes(Arrays.asList(checkNotNull(coreNodes)));
    }

    /**
     * Sets the core nodes.
     *
     * @param nodes the core nodes
     * @return the Atomix builder
     */
    public Builder withNodes(Collection<Node> nodes) {
      config.setNodes(nodes.stream().map(n -> n.config).collect(Collectors.toList()));
      return this;
    }

    /**
     * Enables multicast node discovery.
     *
     * @return the Atomix builder
     */
    public Builder withMulticastEnabled() {
      return withMulticastEnabled(true);
    }

    /**
     * Sets whether multicast node discovery is enabled.
     *
     * @param multicastEnabled whether to enable multicast node discovery
     * @return the Atomix builder
     */
    public Builder withMulticastEnabled(boolean multicastEnabled) {
      config.setMulticastEnabled(multicastEnabled);
      return this;
    }

    /**
     * Sets the multicast address.
     *
     * @param address the multicast address
     * @return the Atomix builder
     */
    public Builder withMulticastAddress(Address address) {
      config.setMulticastAddress(address);
      return this;
    }

    @Override
    public AtomixCluster build() {
      return new AtomixCluster(config);
    }
  }
}
