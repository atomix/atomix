/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.backup.protocol;

import io.atomix.cluster.NodeId;

import java.util.List;

/**
 * Backup request.
 */
public class BackupRequest extends PrimaryBackupRequest {
  private final NodeId primary;
  private final long term;
  private final List<BackupOperation> operations;

  public BackupRequest(NodeId primary, long term, List<BackupOperation> operations) {
    this.primary = primary;
    this.term = term;
    this.operations = operations;
  }

  public NodeId primary() {
    return primary;
  }

  public long term() {
    return term;
  }

  public List<BackupOperation> operations() {
    return operations;
  }
}
