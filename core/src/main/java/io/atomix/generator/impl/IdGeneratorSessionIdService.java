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
package io.atomix.generator.impl;

import io.atomix.cluster.NodeId;
import io.atomix.counter.impl.AtomicCounterProxy;
import io.atomix.generator.AtomicIdGenerator;
import io.atomix.generator.AtomicIdGeneratorType;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.session.ManagedSessionIdService;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.protocols.raft.RaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;
import io.atomix.protocols.raft.proxy.RecoveryStrategy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * ID generator primitive based session ID service.
 */
public class IdGeneratorSessionIdService implements ManagedSessionIdService {
  private static final String PRIMITIVE_NAME = "atomix-primary-elector";
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(NodeId.class)
      .build());

  private final PartitionGroup partitions;
  private AtomicIdGenerator idGenerator;
  private final AtomicBoolean open = new AtomicBoolean();

  public IdGeneratorSessionIdService(PartitionGroup partitionGroup) {
    this.partitions = checkNotNull(partitionGroup);
  }

  @Override
  public SessionId nextSessionId() {
    return SessionId.from(idGenerator.nextId());
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<SessionIdService> open() {
    idGenerator = new DelegatingIdGenerator(new AtomicCounterProxy(partitions.getPartition(PRIMITIVE_NAME)
        .getPrimitiveClient()
        .proxyBuilder(PRIMITIVE_NAME, AtomicIdGeneratorType.instance(), RaftProtocol.builder()
            .withMinTimeout(Duration.ofMillis(250))
            .withMaxTimeout(Duration.ofSeconds(5))
            .withReadConsistency(ReadConsistency.LINEARIZABLE)
            .withCommunicationStrategy(CommunicationStrategy.LEADER)
            .withRecoveryStrategy(RecoveryStrategy.RECOVER)
            .build())
        .withMaxRetries(5)
        .build()))
        .asAtomicIdGenerator();
    open.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  @Override
  public CompletableFuture<Void> close() {
    idGenerator.close();
    open.set(false);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }
}
