/*
 * Copyright 2015-present Open Networking Laboratory
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

import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.error.RaftError;

import java.util.Collection;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Session keep alive response.
 * <p>
 * Session keep alive responses are sent upon the completion of a {@link KeepAliveRequest}
 * from a client. Keep alive responses, when successful, provide the current cluster configuration and leader
 * to the client to ensure clients can evolve with the structure of the cluster and make intelligent decisions
 * about connecting to the cluster.
 */
public class KeepAliveResponse extends AbstractRaftResponse {

  /**
   * Returns a new keep alive response builder.
   *
   * @return A new keep alive response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final MemberId leader;
  private final Collection<MemberId> members;

  public KeepAliveResponse(Status status, RaftError error, MemberId leader, Collection<MemberId> members) {
    super(status, error);
    this.leader = leader;
    this.members = members;
  }

  /**
   * Returns the cluster leader.
   *
   * @return The cluster leader.
   */
  public MemberId leader() {
    return leader;
  }

  /**
   * Returns the cluster members.
   *
   * @return The cluster members.
   */
  public Collection<MemberId> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, leader, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof KeepAliveResponse) {
      KeepAliveResponse response = (KeepAliveResponse) object;
      return response.status == status
          && ((response.leader == null && leader == null)
          || (response.leader != null && leader != null && response.leader.equals(leader)))
          && ((response.members == null && members == null)
          || (response.members != null && members != null && response.members.equals(members)));
    }
    return false;
  }

  @Override
  public String toString() {
    if (status == Status.OK) {
      return toStringHelper(this)
          .add("status", status)
          .add("leader", leader)
          .add("members", members)
          .toString();
    } else {
      return toStringHelper(this)
          .add("status", status)
          .add("error", error)
          .toString();
    }
  }

  /**
   * Status response builder.
   */
  public static class Builder extends AbstractRaftResponse.Builder<Builder, KeepAliveResponse> {
    private MemberId leader;
    private Collection<MemberId> members;

    /**
     * Sets the response leader.
     *
     * @param leader The response leader.
     * @return The response builder.
     */
    public Builder withLeader(MemberId leader) {
      this.leader = leader;
      return this;
    }

    /**
     * Sets the response members.
     *
     * @param members The response members.
     * @return The response builder.
     * @throws NullPointerException if {@code members} is null
     */
    public Builder withMembers(Collection<MemberId> members) {
      this.members = checkNotNull(members, "members cannot be null");
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      if (status == Status.OK) {
        checkNotNull(members, "members cannot be null");
      }
    }

    /**
     * @throws IllegalStateException if status is OK and members is null
     */
    @Override
    public KeepAliveResponse build() {
      validate();
      return new KeepAliveResponse(status, error, leader, members);
    }
  }
}
