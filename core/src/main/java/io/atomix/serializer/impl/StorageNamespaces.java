/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.serializer.impl;

import io.atomix.primitives.counter.impl.RaftCounterOperations;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorEvents;
import io.atomix.primitives.leadership.impl.RaftLeaderElectorOperations;
import io.atomix.primitives.lock.impl.RaftLockEvents;
import io.atomix.primitives.lock.impl.RaftLockOperations;
import io.atomix.primitives.map.impl.RaftAtomicCounterMapOperations;
import io.atomix.primitives.map.impl.RaftConsistentMapEvents;
import io.atomix.primitives.map.impl.RaftConsistentMapOperations;
import io.atomix.primitives.map.impl.RaftConsistentTreeMapOperations;
import io.atomix.primitives.multimap.impl.RaftConsistentSetMultimapEvents;
import io.atomix.primitives.multimap.impl.RaftConsistentSetMultimapOperations;
import io.atomix.primitives.queue.impl.RaftWorkQueueEvents;
import io.atomix.primitives.queue.impl.RaftWorkQueueOperations;
import io.atomix.primitives.tree.impl.RaftDocumentTreeEvents;
import io.atomix.primitives.tree.impl.RaftDocumentTreeOperations;
import io.atomix.primitives.value.impl.RaftValueEvents;
import io.atomix.primitives.value.impl.RaftValueOperations;
import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.cluster.RaftMember;
import io.atomix.protocols.raft.cluster.impl.DefaultRaftMember;
import io.atomix.protocols.raft.event.RaftEvent;
import io.atomix.protocols.raft.event.impl.DefaultEventType;
import io.atomix.protocols.raft.operation.OperationType;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.operation.impl.DefaultOperationId;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.CloseSessionResponse;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.ConfigureRequest;
import io.atomix.protocols.raft.protocol.ConfigureResponse;
import io.atomix.protocols.raft.protocol.HeartbeatRequest;
import io.atomix.protocols.raft.protocol.HeartbeatResponse;
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
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.protocol.ReconfigureRequest;
import io.atomix.protocols.raft.protocol.ReconfigureResponse;
import io.atomix.protocols.raft.protocol.ResetRequest;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.protocols.raft.session.RaftSessionMetadata;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.protocols.raft.storage.log.entry.CloseSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.CommandEntry;
import io.atomix.protocols.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.protocols.raft.storage.log.entry.InitializeEntry;
import io.atomix.protocols.raft.storage.log.entry.KeepAliveEntry;
import io.atomix.protocols.raft.storage.log.entry.MetadataEntry;
import io.atomix.protocols.raft.storage.log.entry.OpenSessionEntry;
import io.atomix.protocols.raft.storage.log.entry.QueryEntry;
import io.atomix.protocols.raft.storage.system.Configuration;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Storage serializer namespaces.
 */
public final class StorageNamespaces {

  /**
   * Raft protocol namespace.
   */
  public static final KryoNamespace RAFT_PROTOCOL = KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(OpenSessionRequest.class)
      .register(OpenSessionResponse.class)
      .register(CloseSessionRequest.class)
      .register(CloseSessionResponse.class)
      .register(KeepAliveRequest.class)
      .register(KeepAliveResponse.class)
      .register(HeartbeatRequest.class)
      .register(HeartbeatResponse.class)
      .register(QueryRequest.class)
      .register(QueryResponse.class)
      .register(CommandRequest.class)
      .register(CommandResponse.class)
      .register(MetadataRequest.class)
      .register(MetadataResponse.class)
      .register(JoinRequest.class)
      .register(JoinResponse.class)
      .register(LeaveRequest.class)
      .register(LeaveResponse.class)
      .register(ConfigureRequest.class)
      .register(ConfigureResponse.class)
      .register(ReconfigureRequest.class)
      .register(ReconfigureResponse.class)
      .register(InstallRequest.class)
      .register(InstallResponse.class)
      .register(PollRequest.class)
      .register(PollResponse.class)
      .register(VoteRequest.class)
      .register(VoteResponse.class)
      .register(AppendRequest.class)
      .register(AppendResponse.class)
      .register(PublishRequest.class)
      .register(ResetRequest.class)
      .register(RaftResponse.Status.class)
      .register(RaftError.class)
      .register(RaftError.Type.class)
      .register(ReadConsistency.class)
      .register(RaftSessionMetadata.class)
      .register(CloseSessionEntry.class)
      .register(CommandEntry.class)
      .register(ConfigurationEntry.class)
      .register(InitializeEntry.class)
      .register(KeepAliveEntry.class)
      .register(MetadataEntry.class)
      .register(OpenSessionEntry.class)
      .register(QueryEntry.class)
      .register(RaftOperation.class)
      .register(RaftEvent.class)
      .register(DefaultEventType.class)
      .register(DefaultOperationId.class)
      .register(OperationType.class)
      .register(ReadConsistency.class)
      .register(ArrayList.class)
      .register(LinkedList.class)
      .register(Collections.emptyList().getClass())
      .register(HashSet.class)
      .register(DefaultRaftMember.class)
      .register(MemberId.class)
      .register(SessionId.class)
      .register(RaftMember.Type.class)
      .register(Instant.class)
      .register(Configuration.class)
      .register(RaftAtomicCounterMapOperations.class)
      .register(RaftConsistentMapEvents.class)
      .register(RaftConsistentMapOperations.class)
      .register(RaftConsistentSetMultimapOperations.class)
      .register(RaftConsistentSetMultimapEvents.class)
      .register(RaftConsistentTreeMapOperations.class)
      .register(RaftCounterOperations.class)
      .register(RaftDocumentTreeEvents.class)
      .register(RaftDocumentTreeOperations.class)
      .register(RaftLeaderElectorEvents.class)
      .register(RaftLeaderElectorOperations.class)
      .register(RaftWorkQueueEvents.class)
      .register(RaftWorkQueueOperations.class)
      .register(RaftValueEvents.class)
      .register(RaftValueOperations.class)
      .register(RaftLockEvents.class)
      .register(RaftLockOperations.class)
      .build("RaftProtocol");

  /**
   * Raft storage namespace.
   */
  public static final KryoNamespace RAFT_STORAGE = KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 100)
      .register(CloseSessionEntry.class)
      .register(CommandEntry.class)
      .register(ConfigurationEntry.class)
      .register(InitializeEntry.class)
      .register(KeepAliveEntry.class)
      .register(MetadataEntry.class)
      .register(OpenSessionEntry.class)
      .register(QueryEntry.class)
      .register(RaftOperation.class)
      .register(ReadConsistency.class)
      .register(ArrayList.class)
      .register(HashSet.class)
      .register(DefaultRaftMember.class)
      .register(MemberId.class)
      .register(RaftMember.Type.class)
      .register(Instant.class)
      .register(Configuration.class)
      .register(RaftAtomicCounterMapOperations.class)
      .register(RaftConsistentMapOperations.class)
      .register(RaftConsistentSetMultimapOperations.class)
      .register(RaftConsistentTreeMapOperations.class)
      .register(RaftCounterOperations.class)
      .register(RaftDocumentTreeOperations.class)
      .register(RaftLeaderElectorOperations.class)
      .register(RaftWorkQueueOperations.class)
      .register(RaftValueOperations.class)
      .register(RaftLockOperations.class)
      .build("RaftStorage");

  private StorageNamespaces() {
  }
}
