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

import io.atomix.cluster.MemberId;
import io.atomix.primitive.Replication;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.event.impl.DefaultEventType;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.protocols.backup.protocol.BackupOperation;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.BackupResponse;
import io.atomix.protocols.backup.protocol.CloseOperation;
import io.atomix.protocols.backup.protocol.CloseRequest;
import io.atomix.protocols.backup.protocol.CloseResponse;
import io.atomix.protocols.backup.protocol.ExecuteOperation;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.ExpireOperation;
import io.atomix.protocols.backup.protocol.HeartbeatOperation;
import io.atomix.protocols.backup.protocol.MetadataRequest;
import io.atomix.protocols.backup.protocol.MetadataResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupResponse;
import io.atomix.protocols.backup.protocol.PrimitiveDescriptor;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.protocol.RestoreResponse;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

/**
 * Primary-backup serializer namespaces.
 */
public final class PrimaryBackupNamespaces {

  /**
   * Primary-backup protocol namespace.
   */
  public static final Namespace PROTOCOL = Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
      .register(MemberId.class)
      .register(PrimaryBackupResponse.Status.class)
      .register(ExecuteRequest.class)
      .register(ExecuteResponse.class)
      .register(BackupRequest.class)
      .register(BackupResponse.class)
      .register(RestoreRequest.class)
      .register(RestoreResponse.class)
      .register(CloseRequest.class)
      .register(CloseResponse.class)
      .register(MetadataRequest.class)
      .register(MetadataResponse.class)
      .register(BackupOperation.Type.class)
      .register(ExecuteOperation.class)
      .register(HeartbeatOperation.class)
      .register(ExpireOperation.class)
      .register(CloseOperation.class)
      .register(PrimitiveDescriptor.class)
      .register(PrimitiveOperation.class)
      .register(PrimitiveEvent.class)
      .register(DefaultEventType.class)
      .register(DefaultOperationId.class)
      .register(OperationType.class)
      .register(Replication.class)
      .build("PrimaryBackupProtocol");

  private PrimaryBackupNamespaces() {
  }
}
