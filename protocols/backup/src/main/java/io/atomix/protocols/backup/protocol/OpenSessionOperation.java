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

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Open session operation.
 */
public class OpenSessionOperation extends BackupOperation {
  private final NodeId nodeId;

  public OpenSessionOperation(long index, long timestamp, NodeId nodeId) {
    super(Type.OPEN_SESSION, index, timestamp);
    this.nodeId = nodeId;
  }

  public NodeId nodeId() {
    return nodeId;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index())
        .add("timestamp", timestamp())
        .add("node", nodeId)
        .toString();
  }
}
