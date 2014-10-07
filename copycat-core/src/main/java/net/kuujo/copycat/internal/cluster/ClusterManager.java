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

import java.util.*;

/**
 * Cluster manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ClusterManager<M extends Member> extends Observable implements Observer {
  private final Cluster<M> cluster;
  private final LocalNode<M> localNode;
  private final Set<RemoteNode<M>> remoteNodes;
  private final Map<String, ClusterNode<M>> nodes;

  public ClusterManager(Cluster<M> cluster) {
    this.cluster = cluster.copy();
    this.localNode = new LocalNode<>(this.cluster.protocol(), this.cluster.localMember());
    this.remoteNodes = new HashSet<>(this.cluster.remoteMembers().size());
    this.nodes = new HashMap<>(this.cluster.members().size());
    this.cluster.addObserver(this);
    clusterChanged(this.cluster);
  }

  @Override
  public void update(Observable o, Object arg) {
    clusterChanged(cluster);
  }

  /**
   * Called when the cluster configuration has changed.
   */
  private void clusterChanged(Cluster<M> cluster) {
    cluster.remoteMembers().forEach(member -> {
      if (!nodes.containsKey(member.id())) {
        RemoteNode<M> node = new RemoteNode<>(cluster.protocol(), member);
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
  public <T extends ClusterNode<M>> T node(String id) {
    return (T) nodes.get(id);
  }

  /**
   * Returns a complete set of all node managers.
   *
   * @return A set of all node managers in the cluster.
   */
  public Set<ClusterNode<M>> nodes() {
    Set<ClusterNode<M>> nodes = new HashSet<>(remoteNodes);
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
    ClusterNode<M> node = nodes.get(id);
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
  public String toString() {
    return String.format("ClusterManager[cluster=%s]", cluster);
  }

}
