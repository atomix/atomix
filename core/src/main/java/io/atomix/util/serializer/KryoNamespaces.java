/*
 * Copyright 2014-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.util.serializer;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.PartitionId;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.CloseSessionResponse;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.ConfigureRequest;
import io.atomix.protocols.raft.protocol.ConfigureResponse;
import io.atomix.protocols.raft.protocol.InstallRequest;
import io.atomix.protocols.raft.protocol.InstallResponse;
import io.atomix.protocols.raft.protocol.JoinRequest;
import io.atomix.protocols.raft.protocol.JoinResponse;
import io.atomix.protocols.raft.protocol.KeepAliveRequest;
import io.atomix.protocols.raft.protocol.KeepAliveResponse;
import io.atomix.protocols.raft.protocol.LeaveRequest;
import io.atomix.protocols.raft.protocol.LeaveResponse;
import io.atomix.protocols.raft.protocol.MetadataRequest;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.OpenSessionRequest;
import io.atomix.protocols.raft.protocol.OpenSessionResponse;
import io.atomix.protocols.raft.protocol.PollRequest;
import io.atomix.protocols.raft.protocol.PollResponse;
import io.atomix.protocols.raft.protocol.PublishRequest;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.ReconfigureRequest;
import io.atomix.protocols.raft.protocol.ReconfigureResponse;
import io.atomix.protocols.raft.protocol.ResetRequest;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.protocols.raft.server.storage.system.Configuration;
import io.atomix.time.LogicalClock;
import io.atomix.time.LogicalTimestamp;
import io.atomix.time.Timestamp;
import io.atomix.time.VectorClock;
import io.atomix.time.VectorTimestamp;
import io.atomix.time.WallClock;
import io.atomix.time.WallClockTimestamp;
import io.atomix.version.Version;
import io.atomix.version.Versioned;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class KryoNamespaces {

    public static final int BASIC_MAX_SIZE = 50;
    public static final KryoNamespace BASIC = KryoNamespace.newBuilder()
            .nextId(KryoNamespace.FLOATING_ID)
            .register(byte[].class)
            .register(AtomicBoolean.class)
            .register(AtomicInteger.class)
            .register(AtomicLong.class)
            .register(Collections.unmodifiableSet(Collections.emptySet()).getClass())
            .register(HashMap.class)
            .register(ConcurrentHashMap.class)
            .register(CopyOnWriteArraySet.class)
            .register(ArrayList.class,
                      LinkedList.class,
                      HashSet.class,
                      LinkedHashSet.class
            )
            .register(HashMultiset.class)
            .register(Maps.immutableEntry("a", "b").getClass())
            .register(Collections.singletonList(1).getClass())
            .register(Duration.class)
            .register(Collections.emptySet().getClass())
            .register(Optional.class)
            .register(Collections.emptyList().getClass())
            .register(Collections.singleton(Object.class).getClass())
            .register(int[].class)
            .register(long[].class)
            .register(short[].class)
            .register(double[].class)
            .register(float[].class)
            .register(char[].class)
            .register(String[].class)
            .register(boolean[].class)
            .build("BASIC");

    /**
     * KryoNamespace which can serialize API bundle classes.
     */
    public static final int API_MAX_SIZE = 499;
    public static final KryoNamespace API = KryoNamespace.newBuilder()
            .nextId(KryoNamespace.INITIAL_ID)
            .register(BASIC)
            .nextId(KryoNamespace.INITIAL_ID + BASIC_MAX_SIZE)
            .register(
                    Node.State.class,
                    NodeId.class,
                    PartitionId.class,
                    Version.class,
                    Versioned.class,
                    Timestamp.class,
                    WallClock.class,
                    LogicalClock.class,
                    VectorClock.class,
                    WallClockTimestamp.class,
                    LogicalTimestamp.class,
                    VectorTimestamp.class
            )
            .build("API");

    public static final KryoNamespace RAFT = KryoNamespace.newBuilder()
            .nextId(KryoNamespace.INITIAL_ID)
            .register(BASIC)
            .nextId(KryoNamespace.INITIAL_ID + BASIC_MAX_SIZE)
            .register(
                    OpenSessionRequest.class,
                    OpenSessionResponse.class,
                    CloseSessionRequest.class,
                    CloseSessionResponse.class,
                    KeepAliveRequest.class,
                    KeepAliveResponse.class,
                    CommandRequest.class,
                    CommandResponse.class,
                    QueryRequest.class,
                    QueryResponse.class,
                    PublishRequest.class,
                    ResetRequest.class,
                    MetadataRequest.class,
                    MetadataResponse.class,
                    JoinRequest.class,
                    JoinResponse.class,
                    LeaveRequest.class,
                    LeaveResponse.class,
                    ConfigureRequest.class,
                    ConfigureResponse.class,
                    ReconfigureRequest.class,
                    ReconfigureResponse.class,
                    InstallRequest.class,
                    InstallResponse.class,
                    PollRequest.class,
                    PollResponse.class,
                    VoteRequest.class,
                    VoteResponse.class,
                    AppendRequest.class,
                    AppendResponse.class,
                    Configuration.class
            )
            .build();

    /**
     * Kryo registration Id for user custom registration.
     */
    public static final int BEGIN_USER_CUSTOM_ID = API_MAX_SIZE + 1;

    // not to be instantiated
    private KryoNamespaces() {
    }
}
