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
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.spi.protocol.Protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Cluster manager.<p>
 *
 * This is an internal helper class which controls connections to all configured nodes within a cluster. Copycat's
 * handling of cluster configuration changes dictates explicit separation from the user-facing
 * {@link net.kuujo.copycat.cluster.Cluster} type. The {@code ClusterManager} allows Copycat to essentially maintain
 * two instances of the cluster configuration, one which is accessible to the user and one which is purely internal
 * and is based on the replicated log. When the {@code ClusterManager} is first constructed from the user-provided
 * {@link net.kuujo.copycat.cluster.Cluster}, the {@code Cluster} is immediately copied and the {@code ClusterManager}
 * begins observing the {@link java.util.Observable} <em>copy</em> of the cluster configuration. This prevents user
 * changes to the external {@link net.kuujo.copycat.cluster.ClusterConfig} from being propagated to the internal
 * {@code ClusterManager} through the observable chain. Instead, the {@code ClusterManager} exposes the copied
 * {@link Cluster} for modification by internal Copycat code. This is the path through which Copycat updates the
 * real cluster configuration.<p>
 *
 * When a Copycat node observes a user cluster configuration change, the change may be appended to the replicated log.
 * All changes to the {@code ClusterManager}'s {@link net.kuujo.copycat.cluster.Cluster} <em>must only be the result
 * of the application of a configuration entry.</em> When the logged configuration is replicated and the entry is applied,
 * the cluster manager's {@link net.kuujo.copycat.cluster.Cluster} configuration is updated and the change is propagated
 * up to the manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterManager<M extends Member> extends Observable implements Observer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterManager.class);
  private final Protocol<M> protocol;
  private final Cluster<M> cluster;
  private final LocalNode<M> localNode;
  private final Set<RemoteNode<M>> remoteNodes;
  private final Map<String, Node<M>> nodes;

  /**
   * @throws NullPointerException if {@code cluster} or {@code protocol} are null
   */
  public ClusterManager(Cluster<M> cluster, Protocol<M> protocol) {
    this.cluster = cluster.copy();
    this.protocol = Assert.isNotNull(protocol, "protocol");
    this.localNode = new LocalNode<>(this.cluster.localMember(), protocol);
    this.remoteNodes = new HashSet<>(this.cluster.remoteMembers().size());
    this.nodes = new HashMap<>(this.cluster.members().size());
    this.cluster.addObserver(this);
    clusterChanged(this.cluster);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void update(Observable o, Object arg) {
    LOGGER.debug("{} - Membership change detected, updating nodes", this);
    clusterChanged((Cluster<M>) o);
  }

  /**
   * Called when the cluster configuration has changed.
   */
  private synchronized void clusterChanged(Cluster<M> cluster) {
    cluster.remoteMembers().forEach(member -> {
      if (!nodes.containsKey(member.id())) {
        RemoteNode<M> node = new RemoteNode<>(member, protocol);
        remoteNodes.add(node);
        nodes.put(member.id(), node);
      }
    });

    Iterator<RemoteNode<M>> iterator = remoteNodes.iterator();
    while (iterator.hasNext()) {
      RemoteNode<M> node = iterator.next();
      boolean exists = false;
      for (M member : cluster.remoteMembers()) {
        if (member.equals(node.member())) {
          exists = true;
          break;
        }
      }
      if (!exists) {
        iterator.remove();
        nodes.remove(node.member().id());
        node.client().close();
      }
    }

    setChanged();
    notifyObservers();
    clearChanged();
  }

  /**
   * Returns the underlying cluster instance.
   *
   * @return The underlying cluster instance.
   */
  public Cluster<M> cluster() {
    return cluster;
  }

  /**
   * Returns a node manager by ID.
   *
   * @param id The unique node manager ID.
   * @param <T> The expected node manager type.
   * @return The node manager instance.
   */
  @SuppressWarnings("unchecked")
  public <T extends Node<M>> T node(String id) {
    return localNode.member().id().equals(id) ? (T) localNode : (T) nodes.get(id);
  }

  /**
   * Returns a complete set of all node managers.
   *
   * @return A set of all node managers in the cluster.
   */
  public Set<Node<M>> nodes() {
    Set<Node<M>> nodes = new HashSet<>(remoteNodes);
    nodes.add(localNode);
    return nodes;
  }

  /**
   * Returns the local node manager.
   *
   * @return The local node manager.
   */
  public LocalNode<M> localNode() {
    return localNode;
  }

  /**
   * Returns a remote node manager by ID.
   *
   * @param id The unique remote node manager ID.
   * @return The remote node manager.
   */
  public RemoteNode<M> remoteNode(String id) {
    Node<M> node = nodes.get(id);
    return node != null && node instanceof RemoteNode ? (RemoteNode<M>) node : null;
  }

  /**
   * Returns a complete set of all remote node managers.
   *
   * @return A complete set of all remote node managers.
   */
  public Set<RemoteNode<M>> remoteNodes() {
    return remoteNodes;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ClusterManager) {
      ClusterManager<?> clusterManager = (ClusterManager<?>) object;
      return clusterManager.localNode.equals(localNode) && clusterManager.remoteNodes.equals(remoteNodes);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 79;
    hashCode = 37 * hashCode + localNode.hashCode();
    hashCode = 37 * hashCode + remoteNodes.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("ClusterManager[cluster=%s]", cluster);
  }

}
