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

import com.google.common.collect.Maps;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Futures;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Test Raft client protocol.
 */
public class TestPrimaryBackupClientProtocol extends TestPrimaryBackupProtocol implements PrimaryBackupClientProtocol {
  private final Map<SessionId, Consumer<PrimitiveEvent>> eventListeners = Maps.newConcurrentMap();

  public TestPrimaryBackupClientProtocol(MemberId memberId, Map<MemberId, TestPrimaryBackupServerProtocol> servers, Map<MemberId, TestPrimaryBackupClientProtocol> clients) {
    super(servers, clients);
    clients.put(memberId, this);
  }

  private CompletableFuture<TestPrimaryBackupServerProtocol> getServer(MemberId memberId) {
    TestPrimaryBackupServerProtocol server = server(memberId);
    if (server != null) {
      return Futures.completedFuture(server);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public CompletableFuture<ExecuteResponse> execute(MemberId memberId, ExecuteRequest request) {
    return getServer(memberId).thenCompose(server -> server.execute(request));
  }

  @Override
  public CompletableFuture<CloseResponse> close(MemberId memberId, CloseRequest request) {
    return getServer(memberId).thenCompose(server -> server.close(request));
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(MemberId memberId, MetadataRequest request) {
    return getServer(memberId).thenCompose(server -> server.metadata(request));
  }

  @Override
  public void registerEventListener(SessionId sessionId, Consumer<PrimitiveEvent> listener, Executor executor) {
    eventListeners.put(sessionId, event -> executor.execute(() -> listener.accept(event)));
  }

  @Override
  public void unregisterEventListener(SessionId sessionId) {
    eventListeners.remove(sessionId);
  }

  void event(SessionId session, PrimitiveEvent event) {
    Consumer<PrimitiveEvent> listener = eventListeners.get(session);
    if (listener != null) {
      listener.accept(event);
    }
  }
}
