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
package io.atomix.primitive.partition;

import io.atomix.cluster.NodeId;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Partition primary term.
 */
public class PrimaryTerm {
  private final long term;
  private final NodeId primary;
  private final List<NodeId> backups;

  public PrimaryTerm(long term, NodeId primary, List<NodeId> backups) {
    this.term = term;
    this.primary = primary;
    this.backups = backups;
  }

  /**
   * Returns the primary term number.
   *
   * @return the primary term number
   */
  public long term() {
    return term;
  }

  /**
   * Returns the primary node identifier.
   *
   * @return the primary node identifier
   */
  public NodeId primary() {
    return primary;
  }

  /**
   * Returns the backup nodes.
   *
   * @return the backup nodes
   */
  public List<NodeId> backups() {
    return backups;
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, primary, backups);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PrimaryTerm) {
      PrimaryTerm term = (PrimaryTerm) object;
      return term.term == this.term
          && Objects.equals(term.primary, primary)
          && Objects.equals(term.backups, backups);
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
