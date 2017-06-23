/*
 * Copyright 2017-present Open Networking Laboratory
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

import io.atomix.logging.Logger;
import io.atomix.protocols.raft.RaftCommand;
import io.atomix.protocols.raft.RaftQuery;
import io.atomix.protocols.raft.error.QueryException;
import io.atomix.protocols.raft.error.UnknownSessionException;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.serializer.Namespace;
import io.atomix.serializer.Serializer;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.ThreadContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Client session submitter test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class RaftProxySubmitterTest {
  private final Serializer serializer = Serializer.using(Namespace.NONE);

  /**
   * Tests submitting a command to the cluster.
   */
  public void testSubmitCommand() throws Throwable {
    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    when(connection.command(any(CommandRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(CommandResponse.newBuilder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world!")
        .build()));

    RaftProxyState state = new RaftProxyState(1, UUID.randomUUID().toString(), "test", 1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxySubmitter submitter = new RaftProxySubmitter(connection, mock(RaftProxyConnection.class), state, new RaftProxySequencer(state), manager, serializer, threadContext);
    assertEquals(submitter.submit(new TestCommand()).get(), "Hello world!");
    assertEquals(state.getCommandRequest(), 1);
    assertEquals(state.getCommandResponse(), 1);
    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Test resequencing a command response.
   */
  public void testResequenceCommand() throws Throwable {
    CompletableFuture<CommandResponse> future1 = new CompletableFuture<>();
    CompletableFuture<CommandResponse> future2 = new CompletableFuture<>();

    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    Mockito.when(connection.command(any(CommandRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    RaftProxyState state = new RaftProxyState(1, UUID.randomUUID().toString(), "test", 1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxySubmitter submitter = new RaftProxySubmitter(connection, mock(RaftProxyConnection.class), state, new RaftProxySequencer(state), manager, serializer, threadContext);

    CompletableFuture<String> result1 = submitter.submit(new TestCommand());
    CompletableFuture<String> result2 = submitter.submit(new TestCommand());

    future2.complete(CommandResponse.newBuilder()
      .withStatus(RaftResponse.Status.OK)
      .withIndex(10)
      .withResult("Hello world again!")
      .build());

    assertEquals(state.getCommandRequest(), 2);
    assertEquals(state.getCommandResponse(), 0);
    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(CommandResponse.newBuilder()
      .withStatus(RaftResponse.Status.OK)
      .withIndex(9)
      .withResult("Hello world!")
      .build());

    assertTrue(result1.isDone());
    assertEquals(result1.get(), "Hello world!");
    assertTrue(result2.isDone());
    assertEquals(result2.get(), "Hello world again!");

    assertEquals(state.getCommandRequest(), 2);
    assertEquals(state.getCommandResponse(), 2);
    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests submitting a query to the cluster.
   */
  public void testSubmitQuery() throws Throwable {
    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    when(connection.query(any(QueryRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(QueryResponse.newBuilder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world!")
        .build()));

    RaftProxyState state = new RaftProxyState(1, UUID.randomUUID().toString(), "test", 1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxySubmitter submitter = new RaftProxySubmitter(mock(RaftProxyConnection.class), connection, state, new RaftProxySequencer(state), manager, serializer, threadContext);
    assertEquals(submitter.submit(new TestQuery()).get(), "Hello world!");
    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests resequencing a query response.
   */
  public void testResequenceQuery() throws Throwable {
    CompletableFuture<QueryResponse> future1 = new CompletableFuture<>();
    CompletableFuture<QueryResponse> future2 = new CompletableFuture<>();

    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    Mockito.when(connection.query(any(QueryRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    RaftProxyState state = new RaftProxyState(1, UUID.randomUUID().toString(), "test", 1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxySubmitter submitter = new RaftProxySubmitter(mock(RaftProxyConnection.class), connection, state, new RaftProxySequencer(state), manager, serializer, threadContext);

    CompletableFuture<String> result1 = submitter.submit(new TestQuery());
    CompletableFuture<String> result2 = submitter.submit(new TestQuery());

    future2.complete(QueryResponse.newBuilder()
      .withStatus(RaftResponse.Status.OK)
      .withIndex(10)
      .withResult("Hello world again!")
      .build());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(QueryResponse.newBuilder()
      .withStatus(RaftResponse.Status.OK)
      .withIndex(9)
      .withResult("Hello world!")
      .build());

    assertTrue(result1.isDone());
    assertEquals(result1.get(), "Hello world!");
    assertTrue(result2.isDone());
    assertEquals(result2.get(), "Hello world again!");

    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests skipping over a failed query attempt.
   */
  public void testSkippingOverFailedQuery() throws Throwable {
    CompletableFuture<QueryResponse> future1 = new CompletableFuture<>();
    CompletableFuture<QueryResponse> future2 = new CompletableFuture<>();

    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    Mockito.when(connection.query(any(QueryRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    RaftProxyState state = new RaftProxyState(1, UUID.randomUUID().toString(), "test", 1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxySubmitter submitter = new RaftProxySubmitter(mock(RaftProxyConnection.class), connection, state, new RaftProxySequencer(state), manager, serializer, threadContext);

    CompletableFuture<String> result1 = submitter.submit(new TestQuery());
    CompletableFuture<String> result2 = submitter.submit(new TestQuery());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.completeExceptionally(new QueryException("failure"));
    future2.complete(QueryResponse.newBuilder()
      .withStatus(RaftResponse.Status.OK)
      .withIndex(10)
      .withResult("Hello world!")
      .build());

    assertTrue(result1.isCompletedExceptionally());
    assertTrue(result2.isDone());
    assertEquals(result2.get(), "Hello world!");

    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests that the client's session is expired when an UnknownSessionException is received from the cluster.
   */
  public void testExpireSessionOnCommandFailure() throws Throwable {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();

    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    Mockito.<CompletableFuture<QueryResponse>>when(connection.query(any(QueryRequest.class)))
      .thenReturn(future);

    RaftProxyState state = new RaftProxyState(1, UUID.randomUUID().toString(), "test", 1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxySubmitter submitter = new RaftProxySubmitter(connection, mock(RaftProxyConnection.class), state, new RaftProxySequencer(state), manager, serializer, threadContext);

    CompletableFuture<String> result = submitter.submit(new TestCommand());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result.isDone());

    future.completeExceptionally(new UnknownSessionException("unknown session"));

    assertTrue(result.isCompletedExceptionally());
  }

  /**
   * Tests that the client's session is expired when an UnknownSessionException is received from the cluster.
   */
  public void testExpireSessionOnQueryFailure() throws Throwable {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();

    RaftProxyConnection connection = mock(RaftProxyConnection.class);
    Mockito.when(connection.query(any(QueryRequest.class)))
      .thenReturn(future);

    RaftProxyState state = new RaftProxyState(1, UUID.randomUUID().toString(), "test", 1000);
    RaftProxyManager manager = mock(RaftProxyManager.class);
    ThreadContext threadContext = new TestContext();

    RaftProxySubmitter submitter = new RaftProxySubmitter(mock(RaftProxyConnection.class), connection, state, new RaftProxySequencer(state), manager, serializer, threadContext);

    CompletableFuture<String> result = submitter.submit(new TestQuery());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result.isDone());

    future.completeExceptionally(new UnknownSessionException("unknown session"));

    assertTrue(result.isCompletedExceptionally());
  }

  /**
   * Test command.
   */
  private static class TestCommand implements RaftCommand<String> {
  }

  /**
   * Test query.
   */
  private static class TestQuery implements RaftQuery<String> {
  }

  /**
   * Test thread context.
   */
  private static class TestContext implements ThreadContext {
    @Override
    public Logger logger() {
      return null;
    }

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
