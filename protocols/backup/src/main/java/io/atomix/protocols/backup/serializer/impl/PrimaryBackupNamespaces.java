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
package io.atomix.protocols.backup.serializer.impl;

import io.atomix.cluster.NodeId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.CloseSessionOperation;
import io.atomix.protocols.backup.protocol.CloseSessionRequest;
import io.atomix.protocols.backup.protocol.CloseSessionResponse;
import io.atomix.protocols.backup.protocol.ExecuteOperation;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.HeartbeatOperation;
import io.atomix.protocols.backup.protocol.MetadataRequest;
import io.atomix.protocols.backup.protocol.MetadataResponse;
import io.atomix.protocols.backup.protocol.OpenSessionOperation;
import io.atomix.protocols.backup.protocol.OpenSessionRequest;
import io.atomix.protocols.backup.protocol.OpenSessionResponse;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.protocol.RestoreResponse;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;

/**
 * Primary-backup serializer namespaces.
 */
public final class PrimaryBackupNamespaces {

  /**
   * Primary-backup protocol namespace.
   */
  public static final KryoNamespace PROTOCOL = KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(NodeId.class)
      .register(OpenSessionRequest.class)
      .register(OpenSessionResponse.class)
      .register(CloseSessionRequest.class)
      .register(CloseSessionResponse.class)
      .register(ExecuteRequest.class)
      .register(ExecuteResponse.class)
      .register(BackupRequest.class)
      .register(RestoreRequest.class)
      .register(RestoreResponse.class)
      .register(MetadataRequest.class)
      .register(MetadataResponse.class)
      .register(OpenSessionOperation.class)
      .register(CloseSessionOperation.class)
      .register(ExecuteOperation.class)
      .register(HeartbeatOperation.class)
      .register(PrimitiveOperation.class)
      .register(DefaultOperationId.class)
      .register(OperationType.class)
      .build("PrimaryBackupProtocol");

  private PrimaryBackupNamespaces() {
  }
}
