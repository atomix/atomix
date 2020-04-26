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
package io.atomix.protocols.raft.session.impl;

import static io.atomix.primitive.operation.PrimitiveOperation.operation;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.RaftError;
import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.TestPrimitiveType;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.storage.buffer.HeapBytes;
import io.atomix.utils.concurrent.BlockingAwareThreadPoolContextFactory;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.ThreadContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client session submitter test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftSessionInvokerTest {
  private static final OperationId COMMAND = OperationId.command("command");
  private static final OperationId QUERY = OperationId.query("query");

  /**
   * Tests submitting a command to the cluster.
   */
  @Test
  public void testSubmitCommand() throws Throwable {
    RaftSessionConnection connection = mock(RaftSessionConnection.class);
    when(connection.command(any(CommandRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(CommandResponse.builder()
            .withStatus(RaftResponse.Status.OK)
            .withIndex(10)
            .withResult("Hello world!".getBytes())
            .build()));

    RaftSessionState state = new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000);
    RaftSessionManager manager = mock(RaftSessionManager.class);
    ThreadContext threadContext = new TestContext();

    RaftSessionInvoker submitter = new RaftSessionInvoker(connection, mock(RaftSessionConnection.class), state, new RaftSessionSequencer(state), manager, threadContext);
    assertArrayEquals(submitter.invoke(operation(COMMAND, HeapBytes.EMPTY)).get(), "Hello world!".getBytes());
    assertEquals(1, state.getCommandRequest());
    assertEquals(1, state.getCommandResponse());
    assertEquals(10, state.getResponseIndex());
  }

  @Test
  public void testReSubmitCommand() throws Throwable {
    // given
    // setup  thread context
    final Logger log = LoggerFactory.getLogger(getClass());
    final int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 16), 4);
    final ThreadContext context = new BlockingAwareThreadPoolContextFactory(
        "raft-partition-group-data-%d", threadPoolSize, log).createContext();

    // collecting request futures
    final List<CompletableFuture<CommandResponse>> futures = new CopyOnWriteArrayList<>();
    final List<Long> sequences = new CopyOnWriteArrayList<>();
    final RaftSessionConnection connection = mock(RaftSessionConnection.class);
    when(connection.command(any(CommandRequest.class)))
        .thenAnswer(invocationOnMock -> {
          final CommandRequest commandRequest = (CommandRequest) invocationOnMock.getArguments()[0];
          sequences.add(commandRequest.sequenceNumber());
          final CompletableFuture<CommandResponse> future = new CompletableFuture<>();
          futures.add(future);
          return future;
        });

    // raft session invoker
    final RaftSessionState state = new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000);
    final RaftSessionManager manager = mock(RaftSessionManager.class);
    final RaftSessionInvoker submitter =
        new RaftSessionInvoker(connection,
            mock(RaftSessionConnection.class),
            state,
            new RaftSessionSequencer(state),
            manager,
            context);

    // send five commands
    submitter.invoke(operation(COMMAND, HeapBytes.EMPTY));
    submitter.invoke(operation(COMMAND, HeapBytes.EMPTY));
    submitter.invoke(operation(COMMAND, HeapBytes.EMPTY));
    submitter.invoke(operation(COMMAND, HeapBytes.EMPTY));
    final CompletableFuture<byte[]> lastFuture = submitter.invoke(operation(COMMAND, HeapBytes.EMPTY));

    verify(connection, timeout(2000).times(5)).command(any());

    // when we missed second command
    futures.get(0).complete(CommandResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world!".getBytes())
        .build());
    final ArrayList<CompletableFuture<CommandResponse>> copiedFutures = new ArrayList<>(futures.subList(1, futures.size()));
    futures.clear();
    sequences.clear();
    copiedFutures.forEach(f ->
        f.complete(CommandResponse.builder()
          .withStatus(RaftResponse.Status.ERROR)
          .withError(RaftError.Type.COMMAND_FAILURE)
          .withLastSequence(1)
          .build()));

    // then session invoker should resubmit 4 commands
    verify(connection, timeout(2000).times(9)).command(any());
    final CountDownLatch latch = new CountDownLatch(1);
    // context thread pool ensures execution of task in order
    context.execute(latch::countDown);
    latch.await();

    assertEquals(4, futures.size());
    assertArrayEquals(new Long[]{2L, 3L, 4L, 5L}, sequences.toArray(new Long[0]));

    futures.forEach(f -> f.complete(CommandResponse.builder()
            .withStatus(RaftResponse.Status.OK)
            .withIndex(10)
            .withResult("Hello world!".getBytes())
            .build()));

    assertArrayEquals(lastFuture.get(), "Hello world!".getBytes());
    assertEquals(5, state.getCommandRequest());
    assertEquals(5, state.getCommandResponse());
    assertEquals(10, state.getResponseIndex());
  }

  /**
   * Test resequencing a command response.
   */
  @Test
  public void testResequenceCommand() throws Throwable {
    CompletableFuture<CommandResponse> future1 = new CompletableFuture<>();
    CompletableFuture<CommandResponse> future2 = new CompletableFuture<>();

    RaftSessionConnection connection = mock(RaftSessionConnection.class);
    Mockito.when(connection.command(any(CommandRequest.class)))
        .thenReturn(future1)
        .thenReturn(future2);

    RaftSessionState state = new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000);
    RaftSessionManager manager = mock(RaftSessionManager.class);
    ThreadContext threadContext = new TestContext();

    RaftSessionInvoker submitter = new RaftSessionInvoker(connection, mock(RaftSessionConnection.class), state, new RaftSessionSequencer(state), manager, threadContext);

    CompletableFuture<byte[]> result1 = submitter.invoke(operation(COMMAND));
    CompletableFuture<byte[]> result2 = submitter.invoke(operation(COMMAND));

    future2.complete(CommandResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world again!".getBytes())
        .build());

    assertEquals(2, state.getCommandRequest());
    assertEquals(0, state.getCommandResponse());
    assertEquals(1, state.getResponseIndex());

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(CommandResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(9)
        .withResult("Hello world!".getBytes())
        .build());

    assertTrue(result1.isDone());
    assertTrue(Arrays.equals(result1.get(), "Hello world!".getBytes()));
    assertTrue(result2.isDone());
    assertTrue(Arrays.equals(result2.get(), "Hello world again!".getBytes()));

    assertEquals(2, state.getCommandRequest());
    assertEquals(2, state.getCommandResponse());
    assertEquals(10, state.getResponseIndex());
  }

  /**
   * Tests submitting a query to the cluster.
   */
  @Test
  public void testSubmitQuery() throws Throwable {
    RaftSessionConnection connection = mock(RaftSessionConnection.class);
    when(connection.query(any(QueryRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(QueryResponse.builder()
            .withStatus(RaftResponse.Status.OK)
            .withIndex(10)
            .withResult("Hello world!".getBytes())
            .build()));

    RaftSessionState state = new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000);
    RaftSessionManager manager = mock(RaftSessionManager.class);
    ThreadContext threadContext = new TestContext();

    RaftSessionInvoker submitter = new RaftSessionInvoker(mock(RaftSessionConnection.class), connection, state, new RaftSessionSequencer(state), manager, threadContext);
    assertTrue(Arrays.equals(submitter.invoke(operation(QUERY)).get(), "Hello world!".getBytes()));
    assertEquals(10, state.getResponseIndex());
  }

  /**
   * Tests resequencing a query response.
   */
  @Test
  public void testResequenceQuery() throws Throwable {
    CompletableFuture<QueryResponse> future1 = new CompletableFuture<>();
    CompletableFuture<QueryResponse> future2 = new CompletableFuture<>();

    RaftSessionConnection connection = mock(RaftSessionConnection.class);
    Mockito.when(connection.query(any(QueryRequest.class)))
        .thenReturn(future1)
        .thenReturn(future2);

    RaftSessionState state = new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000);
    RaftSessionManager manager = mock(RaftSessionManager.class);
    ThreadContext threadContext = new TestContext();

    RaftSessionInvoker submitter = new RaftSessionInvoker(mock(RaftSessionConnection.class), connection, state, new RaftSessionSequencer(state), manager, threadContext);

    CompletableFuture<byte[]> result1 = submitter.invoke(operation(QUERY));
    CompletableFuture<byte[]> result2 = submitter.invoke(operation(QUERY));

    future2.complete(QueryResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world again!".getBytes())
        .build());

    assertEquals(1, state.getResponseIndex());

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(QueryResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(9)
        .withResult("Hello world!".getBytes())
        .build());

    assertTrue(result1.isDone());
    assertTrue(Arrays.equals(result1.get(), "Hello world!".getBytes()));
    assertTrue(result2.isDone());
    assertTrue(Arrays.equals(result2.get(), "Hello world again!".getBytes()));

    assertEquals(10, state.getResponseIndex());
  }

  /**
   * Tests skipping over a failed query attempt.
   */
  @Test
  public void testSkippingOverFailedQuery() throws Throwable {
    CompletableFuture<QueryResponse> future1 = new CompletableFuture<>();
    CompletableFuture<QueryResponse> future2 = new CompletableFuture<>();

    RaftSessionConnection connection = mock(RaftSessionConnection.class);
    Mockito.when(connection.query(any(QueryRequest.class)))
        .thenReturn(future1)
        .thenReturn(future2);

    RaftSessionState state = new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000);
    RaftSessionManager manager = mock(RaftSessionManager.class);
    ThreadContext threadContext = new TestContext();

    RaftSessionInvoker submitter = new RaftSessionInvoker(mock(RaftSessionConnection.class), connection, state, new RaftSessionSequencer(state), manager, threadContext);

    CompletableFuture<byte[]> result1 = submitter.invoke(operation(QUERY));
    CompletableFuture<byte[]> result2 = submitter.invoke(operation(QUERY));

    assertEquals(1, state.getResponseIndex());

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.completeExceptionally(new RaftException.QueryFailure("failure"));
    future2.complete(QueryResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world!".getBytes())
        .build());

    assertTrue(result1.isCompletedExceptionally());
    assertTrue(result2.isDone());
    assertTrue(Arrays.equals(result2.get(), "Hello world!".getBytes()));

    assertEquals(10, state.getResponseIndex());
  }

  /**
   * Tests that the client's session is expired when an UnknownSessionException is received from the cluster.
   */
  @Test
  public void testExpireSessionOnCommandFailure() throws Throwable {
    CompletableFuture<CommandResponse> future = new CompletableFuture<>();

    RaftSessionConnection connection = mock(RaftSessionConnection.class);
    Mockito.when(connection.command(any(CommandRequest.class))).thenReturn(future);

    RaftSessionState state = new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000);
    RaftSessionManager manager = mock(RaftSessionManager.class);
    ThreadContext threadContext = new TestContext();

    RaftSessionInvoker submitter = new RaftSessionInvoker(connection, mock(RaftSessionConnection.class), state, new RaftSessionSequencer(state), manager, threadContext);

    CompletableFuture<byte[]> result = submitter.invoke(operation(COMMAND));

    assertEquals(1, state.getResponseIndex());

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

    RaftSessionConnection connection = mock(RaftSessionConnection.class);
    Mockito.when(connection.query(any(QueryRequest.class)))
        .thenReturn(future);

    RaftSessionState state = new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000);
    RaftSessionManager manager = mock(RaftSessionManager.class);
    ThreadContext threadContext = new TestContext();

    RaftSessionInvoker submitter = new RaftSessionInvoker(mock(RaftSessionConnection.class), connection, state, new RaftSessionSequencer(state), manager, threadContext);

    CompletableFuture<byte[]> result = submitter.invoke(operation(QUERY));

    assertEquals(1, state.getResponseIndex());

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

    @Override
    public boolean isBlocked() {
      return false;
    }

    @Override
    public void block() {

    }

    @Override
    public void unblock() {

    }
  }

}
