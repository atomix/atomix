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

/**
 * Propagation strategy.
 */
public enum PropagationStrategy {

  /**
   * A session that creates a new state for each revision.
   */
  NONE,

  /**
   * A session that creates immutable versions.
   */
  VERSION,

  /**
   * A session that propagates older changes to newer versions.
   */
  PROPAGATE,

  /**
   * A session that copies and isolates state within each version.
   */
  ISOLATE,

}
