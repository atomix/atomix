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
package io.atomix.protocols.backup;

import io.atomix.cluster.NodeId;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Replica info.
 */
public class ReplicaInfo {

  public enum Role {
    PRIMARY,
    BACKUP,
    NONE,
  }

  private final long term;
  private final NodeId primary;
  private final List<NodeId> backups;

  public ReplicaInfo(long term, NodeId primary, List<NodeId> backups) {
    this.term = term;
    this.primary = primary;
    this.backups = backups;
  }

  public long term() {
    return term;
  }

  public NodeId primary() {
    return primary;
  }

  public List<NodeId> backups() {
    return backups;
  }

  public Role roleFor(NodeId nodeId) {
    if (primary.equals(nodeId)) {
      return Role.PRIMARY;
    } else if (backups.contains(nodeId)) {
      return Role.BACKUP;
    } else {
      return Role.NONE;
    }
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ReplicaInfo) {
      ReplicaInfo replicas = (ReplicaInfo) object;
      return replicas.term == term
          && Objects.equals(replicas.primary, primary)
          && Objects.equals(replicas.backups, backups);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("term", term)
        .add("primary", primary)
        .add("backups", backups)
        .toString();
  }
}
