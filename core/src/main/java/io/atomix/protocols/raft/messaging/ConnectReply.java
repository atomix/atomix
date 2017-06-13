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
package io.atomix.protocols.raft.messaging;

import io.atomix.cluster.NodeId;
import io.atomix.protocols.raft.error.RaftError;

import java.util.Collection;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Connect client response.
 * <p>
 * Connect responses are sent in response to a client establishing a new connection with a server.
 * Connect responses do not provide any additional metadata aside from whether or not the request
 * succeeded.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConnectReply extends AbstractRaftReply {

    /**
     * Returns a new connect client response builder.
     *
     * @return A new connect client response builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    private final NodeId leader;
    private final Collection<NodeId> members;

    public ConnectReply(Status status, RaftError error, NodeId leader, Collection<NodeId> members) {
        super(status, error);
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
        return Objects.hash(getClass(), status, leader, members);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof ConnectReply) {
            ConnectReply response = (ConnectReply) object;
            return response.status == status
                    && Objects.equals(response.error, error)
                    && Objects.equals(response.leader, leader)
                    && Objects.equals(response.members, members);
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
     * Connect response builder.
     */
    public static class Builder extends AbstractRaftReply.Builder<Builder, ConnectReply> {
        private NodeId leader;
        private Collection<NodeId> members;

        /**
         * Sets the response leader.
         *
         * @param leader The response leader.
         * @return The response builder.
         */
        public Builder withLeader(NodeId leader) {
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
        public Builder withMembers(Collection<NodeId> members) {
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
        public ConnectReply build() {
            validate();
            return new ConnectReply(status, error, leader, members);
        }
    }
}
