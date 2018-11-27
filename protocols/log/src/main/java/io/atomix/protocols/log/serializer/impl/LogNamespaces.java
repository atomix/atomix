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
package io.atomix.protocols.log.serializer.impl;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.log.LogRecord;
import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.AppendResponse;
import io.atomix.protocols.log.protocol.BackupOperation;
import io.atomix.protocols.log.protocol.BackupRequest;
import io.atomix.protocols.log.protocol.BackupResponse;
import io.atomix.protocols.log.protocol.LogEntry;
import io.atomix.protocols.log.protocol.LogResponse;
import io.atomix.protocols.log.protocol.ConsumeRequest;
import io.atomix.protocols.log.protocol.ConsumeResponse;
import io.atomix.protocols.log.protocol.RecordsRequest;
import io.atomix.protocols.log.protocol.ResetRequest;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

/**
 * Primary-backup serializer namespaces.
 */
public final class LogNamespaces {

  /**
   * Primary-backup protocol namespace.
   */
  public static final Namespace PROTOCOL = Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
      .register(MemberId.class)
      .register(LogResponse.Status.class)
      .register(AppendRequest.class)
      .register(AppendResponse.class)
      .register(ConsumeRequest.class)
      .register(ConsumeResponse.class)
      .register(RecordsRequest.class)
      .register(ResetRequest.class)
      .register(BackupRequest.class)
      .register(BackupResponse.class)
      .register(BackupOperation.class)
      .register(LogEntry.class)
      .register(LogRecord.class)
      .build("LogProtocol");

  private LogNamespaces() {
  }
}
