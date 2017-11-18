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
package io.atomix.protocols.backup.partition.impl;

import com.google.common.collect.Sets;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.protocols.backup.ReplicaInfo;
import io.atomix.protocols.backup.ReplicaInfoProvider;

import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary election based replica info provider.
 */
public class PrimaryElectionReplicaProvider implements ReplicaInfoProvider {
  private final PrimaryElection election;
  private ReplicaInfo replicas;
  private final Set<Consumer<ReplicaInfo>> listeners = Sets.newIdentityHashSet();
  private final PrimaryElectionEventListener listener = this::onElectionEvent;

  public PrimaryElectionReplicaProvider(PrimaryElection election) {
    this.election = checkNotNull(election, "election cannot be null");
    this.replicas = buildReplicas(election.getTerm());
    election.addListener(listener);
  }

  @Override
  public ReplicaInfo replicas() {
    return replicas;
  }

  private void onElectionEvent(PrimaryElectionEvent event) {
    ReplicaInfo replicas = buildReplicas(event.term());
    if (!replicas.equals(this.replicas)) {
      this.replicas = replicas;
      listeners.forEach(l -> l.accept(this.replicas));
    }
  }

  private static ReplicaInfo buildReplicas(PrimaryTerm term) {
    return new ReplicaInfo(term.term(), term.primary(), term.backups());
  }

  @Override
  public void addChangeListener(Consumer<ReplicaInfo> listener) {
    listeners.add(listener);
  }

  @Override
  public void removeChangeListener(Consumer<ReplicaInfo> listener) {
    listeners.remove(listener);
  }

  @Override
  public void close() {
    election.removeListener(listener);
  }
}
