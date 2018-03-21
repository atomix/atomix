/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.proxy.impl;

import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.service.ServiceRevision;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.service.PropagationStrategy;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.storage.buffer.HeapBytes;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.ThreadContext;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Client session submitter test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class RaftProxyInvokerTest {
  private static final OperationId COMMAND = OperationId.command("command");
  private static final OperationId QUERY = OperationId.query("query");

  /**
   * Tests submitting a command to the cluster.
   */
  @Test
  public void testSubmitCommand() throws Throwable {
    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    when(connection.command(any(CommandRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(CommandResponse.newBuilder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world!".getBytes())
        .build()));

    RaftProxyState state = new RaftProxyState(
        "test",
        SessionId.from(1),
        UUID.randomUUID().toString(),
        ServiceType.from("test"),
        new ServiceRevision(1, PropagationStrategy.NONE),
        1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxyInvoker submitter = new RaftProxyInvoker(connection, mock(RaftProxyConnection.class), state, new RaftProxySequencer(state), manager, threadContext);
    assertArrayEquals(submitter.invoke(new RaftOperation(COMMAND, HeapBytes.EMPTY)).get(), "Hello world!".getBytes());
    assertEquals(state.getCommandRequest(), 1);
    assertEquals(state.getCommandResponse(), 1);
    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Test resequencing a command response.
   */
  @Test
  public void testResequenceCommand() throws Throwable {
    CompletableFuture<CommandResponse> future1 = new CompletableFuture<>();
    CompletableFuture<CommandResponse> future2 = new CompletableFuture<>();

    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    Mockito.when(connection.command(any(CommandRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    RaftProxyState state = new RaftProxyState(
        "test",
        SessionId.from(1),
        UUID.randomUUID().toString(),
        ServiceType.from("test"),
        new ServiceRevision(1, PropagationStrategy.NONE),
        1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxyInvoker submitter = new RaftProxyInvoker(connection, mock(RaftProxyConnection.class), state, new RaftProxySequencer(state), manager, threadContext);

    CompletableFuture<byte[]> result1 = submitter.invoke(new RaftOperation(COMMAND, HeapBytes.EMPTY));
    CompletableFuture<byte[]> result2 = submitter.invoke(new RaftOperation(COMMAND, HeapBytes.EMPTY));

    future2.complete(CommandResponse.newBuilder()
      .withStatus(RaftResponse.Status.OK)
      .withIndex(10)
      .withResult("Hello world again!".getBytes())
      .build());

    assertEquals(state.getCommandRequest(), 2);
    assertEquals(state.getCommandResponse(), 0);
    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(CommandResponse.newBuilder()
      .withStatus(RaftResponse.Status.OK)
      .withIndex(9)
      .withResult("Hello world!".getBytes())
      .build());

    assertTrue(result1.isDone());
    assertTrue(Arrays.equals(result1.get(), "Hello world!".getBytes()));
    assertTrue(result2.isDone());
    assertTrue(Arrays.equals(result2.get(), "Hello world again!".getBytes()));

    assertEquals(state.getCommandRequest(), 2);
    assertEquals(state.getCommandResponse(), 2);
    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests submitting a query to the cluster.
   */
  @Test
  public void testSubmitQuery() throws Throwable {
    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    when(connection.query(any(QueryRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(QueryResponse.newBuilder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world!".getBytes())
        .build()));

    RaftProxyState state = new RaftProxyState(
        "test",
        SessionId.from(1),
        UUID.randomUUID().toString(),
        ServiceType.from("test"),
        new ServiceRevision(1, PropagationStrategy.NONE),
        1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxyInvoker submitter = new RaftProxyInvoker(mock(RaftProxyConnection.class), connection, state, new RaftProxySequencer(state), manager, threadContext);
    assertTrue(Arrays.equals(submitter.invoke(new RaftOperation(QUERY, HeapBytes.EMPTY)).get(), "Hello world!".getBytes()));
    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests resequencing a query response.
   */
  @Test
  public void testResequenceQuery() throws Throwable {
    CompletableFuture<QueryResponse> future1 = new CompletableFuture<>();
    CompletableFuture<QueryResponse> future2 = new CompletableFuture<>();

    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    Mockito.when(connection.query(any(QueryRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    RaftProxyState state = new RaftProxyState(
        "test",
        SessionId.from(1),
        UUID.randomUUID().toString(),
        ServiceType.from("test"),
        new ServiceRevision(1, PropagationStrategy.NONE),
        1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxyInvoker submitter = new RaftProxyInvoker(mock(RaftProxyConnection.class), connection, state, new RaftProxySequencer(state), manager, threadContext);

    CompletableFuture<byte[]> result1 = submitter.invoke(new RaftOperation(QUERY, HeapBytes.EMPTY));
    CompletableFuture<byte[]> result2 = submitter.invoke(new RaftOperation(QUERY, HeapBytes.EMPTY));

    future2.complete(QueryResponse.newBuilder()
      .withStatus(RaftResponse.Status.OK)
      .withIndex(10)
      .withResult("Hello world again!".getBytes())
      .build());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(QueryResponse.newBuilder()
      .withStatus(RaftResponse.Status.OK)
      .withIndex(9)
      .withResult("Hello world!".getBytes())
      .build());

    assertTrue(result1.isDone());
    assertTrue(Arrays.equals(result1.get(), "Hello world!".getBytes()));
    assertTrue(result2.isDone());
    assertTrue(Arrays.equals(result2.get(), "Hello world again!".getBytes()));

    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests skipping over a failed query attempt.
   */
  @Test
  public void testSkippingOverFailedQuery() throws Throwable {
    CompletableFuture<QueryResponse> future1 = new CompletableFuture<>();
    CompletableFuture<QueryResponse> future2 = new CompletableFuture<>();

    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    Mockito.when(connection.query(any(QueryRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    RaftProxyState state = new RaftProxyState(
        "test",
        SessionId.from(1),
        UUID.randomUUID().toString(),
        ServiceType.from("test"),
        new ServiceRevision(1, PropagationStrategy.NONE),
        1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxyInvoker submitter = new RaftProxyInvoker(mock(RaftProxyConnection.class), connection, state, new RaftProxySequencer(state), manager, threadContext);

    CompletableFuture<byte[]> result1 = submitter.invoke(new RaftOperation(QUERY, HeapBytes.EMPTY));
    CompletableFuture<byte[]> result2 = submitter.invoke(new RaftOperation(QUERY, HeapBytes.EMPTY));

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.completeExceptionally(new RaftException.QueryFailure("failure"));
    future2.complete(QueryResponse.newBuilder()
      .withStatus(RaftResponse.Status.OK)
      .withIndex(10)
      .withResult("Hello world!".getBytes())
      .build());

    assertTrue(result1.isCompletedExceptionally());
    assertTrue(result2.isDone());
    assertTrue(Arrays.equals(result2.get(), "Hello world!".getBytes()));

    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests that the client's session is expired when an UnknownSessionException is received from the cluster.
   */
  @Test
  public void testExpireSessionOnCommandFailure() throws Throwable {
    CompletableFuture<CommandResponse> future = new CompletableFuture<>();

    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    Mockito.when(connection.command(any(CommandRequest.class))).thenReturn(future);

    RaftProxyState state = new RaftProxyState(
        "test",
        SessionId.from(1),
        UUID.randomUUID().toString(),
        ServiceType.from("test"),
        new ServiceRevision(1, PropagationStrategy.NONE),
        1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxyInvoker submitter = new RaftProxyInvoker(connection, mock(RaftProxyConnection.class), state, new RaftProxySequencer(state), manager, threadContext);

    CompletableFuture<byte[]> result = submitter.invoke(new RaftOperation(COMMAND, HeapBytes.EMPTY));

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result.isDone());

    future.completeExceptionally(new RaftException.UnknownSession("unknown session"));

    assertTrue(result.isCompletedExceptionally());
  }

  /**
   * Tests that the client's session is expired when an UnknownSessionException is received from the cluster.
   */
  @Test
  public void testExpireSessionOnQueryFailure() throws Throwable {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();

    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    Mockito.when(connection.query(any(QueryRequest.class)))
      .thenReturn(future);

    RaftProxyState state = new RaftProxyState(
        "test",
        SessionId.from(1),
        UUID.randomUUID().toString(),
        ServiceType.from("test"),
        new ServiceRevision(1, PropagationStrategy.NONE),
        1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxyInvoker submitter = new RaftProxyInvoker(mock(RaftProxyConnection.class), connection, state, new RaftProxySequencer(state), manager, threadContext);

    CompletableFuture<byte[]> result = submitter.invoke(new RaftOperation(QUERY, HeapBytes.EMPTY));

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result.isDone());

    future.completeExceptionally(new RaftException.UnknownSession("unknown session"));

    assertTrue(result.isCompletedExceptionally());
  }

  /**
   * Test thread context.
   */
  private static class TestContext implements ThreadContext {
    @Override
    public Scheduled schedule(Duration delay, Runnable callback) {
      return null;
    }

    @Override
    public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
      return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void execute(Runnable command) {
      command.run();
    }
  }

}
