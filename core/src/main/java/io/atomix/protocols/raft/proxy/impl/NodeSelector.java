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
 * limitations under the License
 */
package io.atomix.protocols.raft.proxy.impl;

import io.atomix.cluster.NodeId;
import io.atomix.protocols.raft.client.CommunicationStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client address selector.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class NodeSelector implements Iterator<NodeId>, AutoCloseable {

  /**
   * 1
   * Address selector state.
   */
  public enum State {

    /**
     * Indicates that the selector has been reset.
     */
    RESET,

    /**
     * Indicates that the selector is being iterated.
     */
    ITERATE,

    /**
     * Indicates that selector iteration is complete.
     */
    COMPLETE

  }

  private final NodeSelectorManager selectors;
  private NodeId leader;
  private Collection<NodeId> servers = new LinkedList<>();
  private volatile NodeId selection;
  private final CommunicationStrategy strategy;
  private Collection<NodeId> selections = new LinkedList<>();
  private Iterator<NodeId> selectionsIterator;

  public NodeSelector(NodeId leader, Collection<NodeId> servers, CommunicationStrategy strategy, NodeSelectorManager selectors) {
    this.leader = leader;
    this.servers = checkNotNull(servers, "servers cannot be null");
    this.strategy = checkNotNull(strategy, "strategy cannot be null");
    this.selectors = checkNotNull(selectors, "selectors cannot be null");
    this.selections = strategy.selectConnections(leader, new ArrayList<>(servers));
  }

  /**
   * Returns the address selector state.
   *
   * @return The address selector state.
   */
  public State state() {
    if (selectionsIterator == null) {
      return State.RESET;
    } else if (hasNext()) {
      return State.ITERATE;
    } else {
      return State.COMPLETE;
    }
  }

  /**
   * Returns the current address selection.
   *
   * @return The current address selection.
   */
  public NodeId current() {
    return selection;
  }

  /**
   * Returns the current selector leader.
   *
   * @return The current selector leader.
   */
  public NodeId leader() {
    return leader;
  }

  /**
   * Returns the current set of servers.
   *
   * @return The current set of servers.
   */
  public Collection<NodeId> servers() {
    return servers;
  }

  /**
   * Resets the addresses.
   *
   * @return The address selector.
   */
  public NodeSelector reset() {
    if (selectionsIterator != null) {
      this.selections = strategy.selectConnections(leader, new ArrayList<>(servers));
      this.selectionsIterator = null;
    }
    return this;
  }

  /**
   * Resets the connection addresses.
   *
   * @param servers The collection of server addresses.
   * @return The address selector.
   */
  public NodeSelector reset(NodeId leader, Collection<NodeId> servers) {
    if (changed(leader, servers)) {
      this.leader = leader;
      this.servers = servers;
      this.selections = strategy.selectConnections(leader, new ArrayList<>(servers));
      this.selectionsIterator = null;
    }
    return this;
  }

  /**
   * Returns a boolean value indicating whether the selector state would be changed by the given members.
   */
  private boolean changed(NodeId leader, Collection<NodeId> servers) {
    checkNotNull(servers, "servers");
    checkArgument(!servers.isEmpty(), "servers cannot be empty");
    if (this.leader != null && leader == null) {
      return true;
    } else if (this.leader == null && leader != null) {
      checkArgument(servers.contains(leader), "leader must be present in the servers list");
      return true;
    } else if (this.leader != null && !this.leader.equals(leader)) {
      checkArgument(servers.contains(leader), "leader must be present in the servers list");
      return true;
    } else if (!matches(this.servers, servers)) {
      return true;
    }
    return false;
  }

  /**
   * Returns a boolean value indicating whether the servers in the first list match the servers in the second list.
   */
  private boolean matches(Collection<NodeId> left, Collection<NodeId> right) {
    if (left.size() != right.size())
      return false;

    for (NodeId address : left) {
      if (!right.contains(address)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean hasNext() {
    return selectionsIterator == null ? !selections.isEmpty() : selectionsIterator.hasNext();
  }

  @Override
  public NodeId next() {
    if (selectionsIterator == null) {
      selectionsIterator = selections.iterator();
    }
    NodeId selection = selectionsIterator.next();
    this.selection = selection;
    return selection;
  }

  @Override
  public void close() {
    selectors.remove(this);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("strategy", strategy)
        .toString();
  }

}
