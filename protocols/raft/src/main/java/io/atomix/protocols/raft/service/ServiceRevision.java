/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.protocols.raft.service;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Service revision.
 */
public final class ServiceRevision {
  private final int revision;
  private final PropagationStrategy propagationStrategy;

  public ServiceRevision(int revision, PropagationStrategy propagationStrategy) {
    this.revision = revision;
    this.propagationStrategy = propagationStrategy;
  }

  /**
   * Returns the revision number.
   *
   * @return the revision number
   */
  public int revision() {
    return revision;
  }

  /**
   * Returns the revision propagation strategy.
   *
   * @return the revision propagation strategy
   */
  public PropagationStrategy propagationStrategy() {
    return propagationStrategy;
  }

  @Override
  public int hashCode() {
    return Objects.hash(revision, propagationStrategy);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof ServiceRevision
        && ((ServiceRevision) object).revision == revision;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("revision", revision)
        .add("strategy", propagationStrategy)
        .toString();
  }
}
