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
import io.atomix.utils.event.ListenerService;

import java.util.concurrent.CompletableFuture;

/**
 * Partition primary election.
 */
public interface PrimaryElection extends ListenerService<PrimaryElectionEvent, PrimaryElectionEventListener> {

  /**
   * Enters the primary election.
   *
   * @param nodeId the identifier of the node to enter the election
   * @return the current term
   */
  CompletableFuture<PrimaryTerm> enter(NodeId nodeId);

  /**
   * Returns the current term.
   *
   * @return the current term
   */
  CompletableFuture<PrimaryTerm> getTerm();

  /**
   * Closes the primary election.
   */
  void close();

}
