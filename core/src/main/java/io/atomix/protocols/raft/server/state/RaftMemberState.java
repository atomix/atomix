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
package io.atomix.protocols.raft.server.state;

import com.google.common.hash.Hashing;
import io.atomix.cluster.NodeId;
import io.atomix.protocols.raft.error.RaftError;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.protocol.ReconfigureRequest;
import io.atomix.protocols.raft.server.cluster.RaftMember;
import io.atomix.protocols.raft.server.storage.system.Configuration;
import io.atomix.util.Assert;
import io.atomix.util.temp.Listener;
import io.atomix.util.temp.Listeners;
import io.atomix.util.temp.Scheduled;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class RaftMemberState implements RaftMember, AutoCloseable {
    private final NodeId id;
    private final transient int hash;
    private RaftMember.Type type;
    private Status status = Status.AVAILABLE;
    private Instant updated;
    private transient Scheduled configureTimeout;
    private transient RaftClusterState cluster;
    private transient Listeners<Type> typeChangeListeners;
    private transient Listeners<Status> statusChangeListeners;

    public RaftMemberState(NodeId id, RaftMember.Type type, RaftMember.Status status, Instant updated) {
        this.id = Assert.notNull(id, "id");
        this.hash = Hashing.murmur3_32()
                .hashUnencodedChars(id.id())
                .asInt();
        this.type = Assert.notNull(type, "type");
        this.status = Assert.notNull(status, "status");
        this.updated = Assert.notNull(updated, "updated");
    }

    /**
     * Sets the member's parent cluster.
     */
    RaftMemberState setCluster(RaftClusterState cluster) {
        this.cluster = cluster;
        return this;
    }

    @Override
    public NodeId id() {
        return id;
    }

    @Override
    public int hash() {
        return 0;
    }

    @Override
    public RaftMember.Type type() {
        return type;
    }

    @Override
    public Status status() {
        return status;
    }

    @Override
    public Instant updated() {
        return updated;
    }

    @Override
    public Listener<Type> onTypeChange(Consumer<Type> callback) {
        if (typeChangeListeners == null)
            typeChangeListeners = new Listeners<>();
        return typeChangeListeners.add(callback);
    }

    @Override
    public Listener<Status> onStatusChange(Consumer<Status> callback) {
        if (statusChangeListeners == null)
            statusChangeListeners = new Listeners<>();
        return statusChangeListeners.add(callback);
    }

    @Override
    public CompletableFuture<Void> promote() {
        return configure(Type.values()[type.ordinal() + 1]);
    }

    @Override
    public CompletableFuture<Void> promote(Type type) {
        return configure(type);
    }

    @Override
    public CompletableFuture<Void> demote() {
        return configure(Type.values()[type.ordinal() - 1]);
    }

    @Override
    public CompletableFuture<Void> demote(Type type) {
        return configure(type);
    }

    @Override
    public CompletableFuture<Void> remove() {
        return configure(Type.INACTIVE);
    }

    /**
     * Updates the member type.
     *
     * @param type The member type.
     * @return The member.
     */
    RaftMemberState update(RaftMember.Type type, Instant time) {
        if (this.type != type) {
            this.type = Assert.notNull(type, "type");
            if (time.isAfter(updated)) {
                this.updated = Assert.notNull(time, "time");
            }
            if (typeChangeListeners != null) {
                typeChangeListeners.accept(type);
            }
        }
        return this;
    }

    /**
     * Updates the member status.
     *
     * @param status The member status.
     * @return The member.
     */
    RaftMemberState update(Status status, Instant time) {
        if (this.status != status) {
            this.status = Assert.notNull(status, "status");
            if (time.isAfter(updated)) {
                this.updated = Assert.notNull(time, "time");
            }
            if (statusChangeListeners != null) {
                statusChangeListeners.accept(status);
            }
        }
        return this;
    }

    /**
     * Demotes the server to the given type.
     */
    CompletableFuture<Void> configure(RaftMember.Type type) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        cluster.getContext().getThreadContext().execute(() -> configure(type, future));
        return future;
    }

    /**
     * Recursively reconfigures the cluster.
     */
    private void configure(RaftMember.Type type, CompletableFuture<Void> future) {
        // Set a timer to retry the attempt to leave the cluster.
        configureTimeout = cluster.getContext().getThreadContext().schedule(cluster.getContext().getElectionTimeout(), () -> {
            configure(type, future);
        });

        // Attempt to leave the cluster by submitting a LeaveRequest directly to the server state.
        // Non-leader states should forward the request to the leader if there is one. Leader states
        // will log, replicate, and commit the reconfiguration.
        cluster.getContext().getServerState().reconfigure(ReconfigureRequest.builder()
                .withIndex(cluster.getConfiguration().index())
                .withTerm(cluster.getConfiguration().term())
                .withMember(new RaftMemberState(id, type, status, updated))
                .build()).whenComplete((response, error) -> {
            if (error == null) {
                if (response.status() == RaftResponse.Status.OK) {
                    cancelConfigureTimer();
                    cluster.configure(new Configuration(response.index(), response.term(), response.timestamp(), response.members()));
                    future.complete(null);
                } else if (response.error() == null || response.error() == RaftError.Type.NO_LEADER_ERROR) {
                    cancelConfigureTimer();
                    configureTimeout = cluster.getContext().getThreadContext().schedule(cluster.getContext().getElectionTimeout().multipliedBy(2), () -> configure(type, future));
                } else {
                    cancelConfigureTimer();
                    future.completeExceptionally(response.error().createException());
                }
            }
        });
    }

    /**
     * Cancels the configure timeout.
     */
    private void cancelConfigureTimer() {
        if (configureTimeout != null) {
            configureTimeout.cancel();
            configureTimeout = null;
        }
    }

    @Override
    public void close() {
        cancelConfigureTimer();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), id);
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof RaftMemberState && ((RaftMemberState) object).id.equals(id);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", id)
                .add("type", type)
                .add("status", status)
                .add("updated", updated)
                .toString();
    }

}
