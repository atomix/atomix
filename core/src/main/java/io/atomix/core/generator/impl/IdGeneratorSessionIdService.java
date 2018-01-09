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
package io.atomix.core.generator.impl;

import io.atomix.core.counter.impl.AtomicCounterProxy;
import io.atomix.core.generator.AsyncAtomicIdGenerator;
import io.atomix.core.generator.AtomicIdGeneratorType;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.session.ManagedSessionIdService;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.protocols.raft.RaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * ID generator primitive based session ID service.
 */
public class IdGeneratorSessionIdService implements ManagedSessionIdService {
  private static final String PRIMITIVE_NAME = "atomix-session-ids";

  private final PartitionGroup partitions;
  private AsyncAtomicIdGenerator idGenerator;
  private final AtomicBoolean started = new AtomicBoolean();

  public IdGeneratorSessionIdService(PartitionGroup partitionGroup) {
    this.partitions = checkNotNull(partitionGroup);
  }

  @Override
  public CompletableFuture<SessionId> nextSessionId() {
    return idGenerator.nextId().thenApply(SessionId::from);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<SessionIdService> start() {
    PrimitiveProxy proxy = partitions.getPartition(PRIMITIVE_NAME)
        .getPrimitiveClient()
        .newProxy(PRIMITIVE_NAME, AtomicIdGeneratorType.instance(), RaftProtocol.builder()
            .withMinTimeout(Duration.ofMillis(250))
            .withMaxTimeout(Duration.ofSeconds(5))
            .withReadConsistency(ReadConsistency.LINEARIZABLE)
            .withCommunicationStrategy(CommunicationStrategy.LEADER)
            .withRecoveryStrategy(Recovery.RECOVER)
            .withMaxRetries(5)
            .build());
    return proxy.connect()
        .thenApply(v -> {
          idGenerator = new DelegatingAtomicIdGenerator(new AtomicCounterProxy(proxy));
          started.set(true);
          return this;
        });
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    idGenerator.close();
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
