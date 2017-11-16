/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.protocols.raft.protocol;

import io.atomix.cluster.NodeId;

import java.util.Collection;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client heartbeat request.
 */
public class HeartbeatRequest extends AbstractRaftRequest {

  /**
   * Returns a new heartbeat request builder.
   *
   * @return A new heartbeat request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final NodeId leader;
  private final Collection<NodeId> members;

  public HeartbeatRequest(NodeId leader, Collection<NodeId> members) {
    this.leader = leader;
    this.members = members;
  }

  /**
   * Returns the cluster leader.
   *
   * @return The cluster leader.
   */
  public NodeId leader() {
    return leader;
  }

  /**
   * Returns the cluster members.
   *
   * @return The cluster members.
   */
  public Collection<NodeId> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), leader, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof HeartbeatRequest) {
      HeartbeatRequest request = (HeartbeatRequest) object;
      return Objects.equals(request.leader, leader) && Objects.equals(request.members, members);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("leader", leader)
        .add("members", members)
        .toString();
  }

  /**
   * Heartbeat request builder.
   */
  public static class Builder extends AbstractRaftRequest.Builder<Builder, HeartbeatRequest> {
    private NodeId leader;
    private Collection<NodeId> members;

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The request builder.
     */
    public Builder withLeader(NodeId leader) {
      this.leader = leader;
      return this;
    }

    /**
     * Sets the request members.
     *
     * @param members The request members.
     * @return The request builder.
     * @throws NullPointerException if {@code members} is null
     */
    public Builder withMembers(Collection<NodeId> members) {
      this.members = checkNotNull(members, "members cannot be null");
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkNotNull(members, "members cannot be null");
    }

    /**
     * @throws IllegalStateException if status is OK and members is null
     */
    @Override
    public HeartbeatRequest build() {
      validate();
      return new HeartbeatRequest(leader, members);
    }
  }
}
