// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.protocol;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import io.atomix.cluster.MemberId;
import io.atomix.utils.concurrent.Futures;

/**
 * Test Raft client protocol.
 */
public class TestLogClientProtocol extends TestLogProtocol implements LogClientProtocol {
  private final Map<String, Consumer<RecordsRequest>> consumers = new ConcurrentHashMap<>();

  public TestLogClientProtocol(MemberId memberId, Map<MemberId, TestLogServerProtocol> servers, Map<MemberId, TestLogClientProtocol> clients) {
    super(servers, clients);
    clients.put(memberId, this);
  }

  private CompletableFuture<TestLogServerProtocol> getServer(MemberId memberId) {
    TestLogServerProtocol server = server(memberId);
    if (server != null) {
      return Futures.completedFuture(server);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(MemberId memberId, AppendRequest request) {
    return getServer(memberId).thenCompose(server -> server.append(request));
  }

  @Override
  public CompletableFuture<ConsumeResponse> consume(MemberId memberId, ConsumeRequest request) {
    return getServer(memberId).thenCompose(server -> server.consume(request));
  }

  @Override
  public void reset(MemberId memberId, ResetRequest request) {
    getServer(memberId).thenAccept(server -> server.reset(request));
  }

  @Override
  public void registerRecordsConsumer(String subject, Consumer<RecordsRequest> handler, Executor executor) {
    consumers.put(subject, request -> executor.execute(() -> handler.accept(request)));
  }

  @Override
  public void unregisterRecordsConsumer(String subject) {
    consumers.remove(subject);
  }

  void produce(String subject, RecordsRequest request) {
    Consumer<RecordsRequest> consumer = consumers.get(subject);
    if (consumer != null) {
      consumer.accept(request);
    }
  }
}
