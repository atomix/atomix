// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
