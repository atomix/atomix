/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.protocol;

import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.testng.Assert.*;

/**
 * Request/response tests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class RequestResponseTest {

  /**
   * Tests that the commit request builder when not configured.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitRequestBuilderFailsWithoutConfiguration() {
    CommitRequest.builder().build();
  }

  /**
   * Tests that the commit request builder fails without a configured ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitRequestBuilderFailsWithoutId() {
    CommitRequest.builder().withUri("foo").withEntry(ByteBuffer.wrap("Hello world!".getBytes())).build();
  }

  /**
   * Tests that the commit request builder fails with a null ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitRequestBuilderFailsWithNullId() {
    CommitRequest.builder().withId(null).build();
  }

  /**
   * Tests that the commit request builder fails without a configured member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitRequestBuilderFailsWithoutMember() {
    CommitRequest.builder().withId("test").withEntry(ByteBuffer.wrap("Hello world!".getBytes())).build();
  }

  /**
   * Tests that the commit request builder fails with a null member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitRequestBuilderFailsWithNullMember() {
    CommitRequest.builder().withUri(null).build();
  }

  /**
   * Tests that the commit request builder fails without a configured entry.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitRequestBuilderFailsWithoutEntry() {
    CommitRequest.builder().withId("test").withUri("foo").build();
  }

  /**
   * Tests that the commit request builder fails when no entry has been set.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitRequestBuilderEntrySetterFailsWithNullEntry() {
    CommitRequest.builder().withEntry(null).build();
  }

  /**
   * Tests that the commit request builder succeeds when properly configured.
   */
  public void testCommitRequestBuilderSucceedsWithValidConfiguration() {
    CommitRequest request = CommitRequest.builder()
      .withId("test")
      .withUri("foo")
      .withEntry(ByteBuffer.wrap("Hello world!".getBytes())).build();
    assertEquals(request.id(), "test");
    assertEquals(request.uri(), "foo");
    assertEquals(new String(request.entry().array()), "Hello world!");
  }

  /**
   * Tests that the commit response builder fails without being properly configured.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitResponseBuilderFailsWithoutConfiguration() {
    CommitResponse.builder().build();
  }

  /**
   * Tests that the commit response builder fails without an ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitResponseBuilderFailsWithoutId() {
    CommitResponse.builder().withUri("foo").withResult("Hello world!".getBytes()).build();
  }

  /**
   * Tests that the commit response builder fails with a null ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitResponseBuilderFailsWithNullId() {
    CommitResponse.builder().withId(null).build();
  }

  /**
   * Tests that the commit response builder fails without a member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitResponseBuilderFailsWithoutMember() {
    CommitResponse.builder().withId("test").withResult("Hello world!".getBytes()).build();
  }

  /**
   * Tests that the commit response builder fails with a null member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitResponseBuilderFailsWithNullMember() {
    CommitResponse.builder().withUri(null).build();
  }

  /**
   * Tests that the commit response builder succeeds with a null result.
   */
  public void testCommitResponseBuilderSucceedsWithNullResult() {
    CommitResponse response = CommitResponse.builder()
      .withId("test")
      .withUri("foo")
      .withResult(null)
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.uri(), "foo");
    assertNull(response.result());
  }

  /**
   * Tests that the commit response builder succeeds with a valid configuration.
   */
  public void testCommitResponseBuilderSucceedsWithValidConfiguration() {
    CommitResponse response = CommitResponse.builder()
      .withId("test")
      .withUri("foo")
      .withResult("Hello world!".getBytes())
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.uri(), "foo");
    assertEquals(new String(response.result()), "Hello world!");
  }

  /**
   * Tests that the append request builder when not configured.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRequestBuilderFailsWithoutConfiguration() {
    AppendRequest.builder().build();
  }

  /**
   * Tests that the append request builder fails without a configured ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRequestBuilderFailsWithoutId() {
    AppendRequest.builder()
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails with a null ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRequestBuilderFailsWithNullId() {
    AppendRequest.builder()
      .withId(null)
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails without a configured member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRequestBuilderFailsWithoutMember() {
    AppendRequest.builder()
      .withId("test")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails with a null member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRequestBuilderFailsWithNullMember() {
    AppendRequest.builder()
      .withId("test")
      .withUri(null)
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails without a leader.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRequestBuilderFailsWithoutLeader() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails with a null leader.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRequestBuilderFailsWithNullLeader() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader(null)
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails without a term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAppendRequestBuilderFailsWithoutTerm() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails without an invalid term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAppendRequestBuilderFailsWithInvalidTerm() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(-1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails without entries.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRequestBuilderFailsWithoutEntries() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails without null entries.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRequestBuilderFailsWithNullEntries() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries((List<ByteBuffer>) null)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails with an invalid log index.
   */
  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testAppendRequestBuilderFailsWithInvalidLogIndex() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(-1L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails with an invalid log term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAppendRequestBuilderFailsWithInvalidLogTerm() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(-1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder succeeds with a null log index and term.
   */
  public void testAppendRequestBuilderSucceedsWithNullLogIndexAndNullLogTerm() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(null)
      .withLogTerm(null)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails with a valid log index and null log term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAppendRequestBuilderFailsWithValidLogIndexAndNullLogTerm() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(null)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder fails with a null log index and valid log term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAppendRequestBuilderFailsWithNullLogIndexAndValidLogTerm() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(null)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the append request builder succeeds with a null commit index.
   */
  public void testAppendRequestBuilderSucceedsWithNullCommitIndex() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(null)
      .build();
  }

  /**
   * Tests that the append request builder fails with an invalid commit index.
   */
  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testAppendRequestBuilderFailsWithInvalidCommitIndex() {
    AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(-1L)
      .build();
  }

  /**
   * Tests that the append request builder succeeds when properly configured.
   */
  public void testAppendRequestBuilderSucceedsWithValidConfiguration() {
    AppendRequest request = AppendRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
    assertEquals(request.id(), "test");
    assertEquals(request.uri(), "foo");
    assertEquals(request.leader(), "bar");
    assertEquals(request.term(), 1);
    assertEquals(new String(request.entries().get(0).array()), "Hello world!");
    assertEquals(request.logIndex().longValue(), 5);
    assertEquals(request.logTerm().longValue(), 1);
    assertEquals(request.commitIndex().longValue(), 4);
  }

  /**
   * Tests that the append response builder fails without being properly configured.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendResponseBuilderFailsWithoutConfiguration() {
    AppendResponse.builder().build();
  }

  /**
   * Tests that the append response builder fails without an ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendResponseBuilderFailsWithoutId() {
    AppendResponse.builder()
      .withUri("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
  }

  /**
   * Tests that the append response builder fails with a null ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendResponseBuilderFailsWithNullId() {
    AppendResponse.builder()
      .withId(null)
      .withUri("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
  }

  /**
   * Tests that the append response builder fails without a member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendResponseBuilderFailsWithoutMember() {
    AppendResponse.builder()
      .withId("test")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
  }

  /**
   * Tests that the append response builder fails with a null member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendResponseBuilderFailsWithNullMember() {
    AppendResponse.builder()
      .withId("test")
      .withUri(null)
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
  }

  /**
   * Tests that the append response builder fails with an invalid term.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendResponseBuilderFailsWithInvalidTerm() {
    AppendResponse.builder()
      .withId("test")
      .withUri(null)
      .withTerm(-1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
  }

  /**
   * Tests that the append response builder succeeds with a null index.
   */
  public void testAppendResponseBuilderSucceedsWithNullIndex() {
    AppendResponse.builder()
      .withId("test")
      .withUri("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(null)
      .build();
  }

  /**
   * Tests that the append response builder fails with an invalid index.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendResponseBuilderFailsWithInvalidIndex() {
    AppendResponse.builder()
      .withId("test")
      .withUri(null)
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(-1L)
      .build();
  }

  /**
   * Tests that the append response builder succeeds with a valid configuration.
   */
  public void testAppendResponseBuilderSucceedsWithValidConfiguration() {
    AppendResponse response = AppendResponse.builder()
      .withId("test")
      .withUri("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.uri(), "foo");
    assertEquals(response.term(), 1);
    assertTrue(response.succeeded());
    assertEquals(response.logIndex().longValue(), 4);
  }

  /**
   * Tests that the ping request builder when not configured.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingRequestBuilderFailsWithoutConfiguration() {
    PingRequest.builder().build();
  }

  /**
   * Tests that the ping request builder fails without a configured ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingRequestBuilderFailsWithoutId() {
    PingRequest.builder()
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder fails with a null ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingRequestBuilderFailsWithNullId() {
    PingRequest.builder()
      .withId(null)
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder fails without a configured member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingRequestBuilderFailsWithoutMember() {
    PingRequest.builder()
      .withId("test")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder fails with a null member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingRequestBuilderFailsWithNullMember() {
    PingRequest.builder()
      .withId("test")
      .withUri(null)
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder fails without a leader.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingRequestBuilderFailsWithoutLeader() {
    PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder fails with a null leader.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingRequestBuilderFailsWithNullLeader() {
    PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader(null)
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder fails without a term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPingRequestBuilderFailsWithoutTerm() {
    PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder fails without an invalid term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPingRequestBuilderFailsWithInvalidTerm() {
    PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(-1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder fails with an invalid log index.
   */
  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testPingRequestBuilderFailsWithInvalidLogIndex() {
    PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(-1L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder fails with an invalid log term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPingRequestBuilderFailsWithInvalidLogTerm() {
    PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(-1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder succeeds with a null log index and term.
   */
  public void testPingRequestBuilderSucceedsWithNullLogIndexAndNullLogTerm() {
    PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(null)
      .withLogTerm(null)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder fails with a valid log index and null log term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPingRequestBuilderFailsWithValidLogIndexAndNullLogTerm() {
    PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(null)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder fails with a null log index and valid log term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPingRequestBuilderFailsWithNullLogIndexAndValidLogTerm() {
    PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(null)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
  }

  /**
   * Tests that the ping request builder succeeds with a null commit index.
   */
  public void testPingRequestBuilderSucceedsWithNullCommitIndex() {
    PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(null)
      .build();
  }

  /**
   * Tests that the ping request builder fails with an invalid commit index.
   */
  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testPingRequestBuilderFailsWithInvalidCommitIndex() {
    PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(-1L)
      .build();
  }

  /**
   * Tests that the ping request builder succeeds when properly configured.
   */
  public void testPingRequestBuilderSucceedsWithValidConfiguration() {
    PingRequest request = PingRequest.builder()
      .withId("test")
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
    assertEquals(request.id(), "test");
    assertEquals(request.uri(), "foo");
    assertEquals(request.leader(), "bar");
    assertEquals(request.term(), 1);
    assertEquals(request.logIndex().longValue(), 5);
    assertEquals(request.logTerm().longValue(), 1);
    assertEquals(request.commitIndex().longValue(), 4);
  }

  /**
   * Tests that the ping response builder fails without being properly configured.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingResponseBuilderFailsWithoutConfiguration() {
    PingResponse.builder().build();
  }

  /**
   * Tests that the ping response builder fails without an ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingResponseBuilderFailsWithoutId() {
    PingResponse.builder()
      .withUri("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
  }

  /**
   * Tests that the ping response builder fails with a null ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingResponseBuilderFailsWithNullId() {
    PingResponse.builder()
      .withId(null)
      .withUri("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
  }

  /**
   * Tests that the ping response builder fails without a member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingResponseBuilderFailsWithoutMember() {
    PingResponse.builder()
      .withId("test")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
  }

  /**
   * Tests that the ping response builder fails with a null member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingResponseBuilderFailsWithNullMember() {
    PingResponse.builder()
      .withId("test")
      .withUri(null)
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
  }

  /**
   * Tests that the ping response builder fails with an invalid term.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingResponseBuilderFailsWithInvalidTerm() {
    PingResponse.builder()
      .withId("test")
      .withUri(null)
      .withTerm(-1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
  }

  /**
   * Tests that the ping response builder succeeds with a null index.
   */
  public void testPingResponseBuilderSucceedsWithNullIndex() {
    PingResponse.builder()
      .withId("test")
      .withUri("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(null)
      .build();
  }

  /**
   * Tests that the ping response builder fails with an invalid index.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPingResponseBuilderFailsWithInvalidIndex() {
    PingResponse.builder()
      .withId("test")
      .withUri(null)
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(-1L)
      .build();
  }

  /**
   * Tests that the ping response builder succeeds with a valid configuration.
   */
  public void testPingResponseBuilderSucceedsWithValidConfiguration() {
    PingResponse response = PingResponse.builder()
      .withId("test")
      .withUri("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.uri(), "foo");
    assertEquals(response.term(), 1);
    assertTrue(response.succeeded());
    assertEquals(response.logIndex().longValue(), 4);
  }

  /**
   * Tests that the poll request builder when not configured.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollRequestBuilderFailsWithoutConfiguration() {
    PollRequest.builder().build();
  }

  /**
   * Tests that the poll request builder fails without a configured ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollRequestBuilderFailsWithoutId() {
    PollRequest.builder()
      .withUri("foo")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .build();
  }

  /**
   * Tests that the poll request builder fails with a null ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollRequestBuilderFailsWithNullId() {
    PollRequest.builder()
      .withId(null)
      .withUri("foo")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .build();
  }

  /**
   * Tests that the poll request builder fails without a configured member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollRequestBuilderFailsWithoutMember() {
    PollRequest.builder()
      .withId("test")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .build();
  }

  /**
   * Tests that the poll request builder fails with a null member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollRequestBuilderFailsWithNullMember() {
    PollRequest.builder()
      .withId("test")
      .withUri(null)
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .build();
  }

  /**
   * Tests that the poll request builder fails without a candidate.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollRequestBuilderFailsWithoutCandidate() {
    PollRequest.builder()
      .withId("test")
      .withUri("foo")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .build();
  }

  /**
   * Tests that the poll request builder fails with a null candidate.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollRequestBuilderFailsWithNullCandidate() {
    PollRequest.builder()
      .withId("test")
      .withUri("foo")
      .withCandidate(null)
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .build();
  }

  /**
   * Tests that the poll request builder fails without a term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPollRequestBuilderFailsWithoutTerm() {
    PollRequest.builder()
      .withId("test")
      .withUri("foo")
      .withCandidate("bar")
      .withLogIndex(5L)
      .withLogTerm(1L)
      .build();
  }

  /**
   * Tests that the poll request builder fails without an invalid term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPollRequestBuilderFailsWithInvalidTerm() {
    PollRequest.builder()
      .withId("test")
      .withUri("foo")
      .withCandidate("bar")
      .withTerm(-1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .build();
  }

  /**
   * Tests that the poll request builder fails with an invalid log index.
   */
  @Test(expectedExceptions = IndexOutOfBoundsException.class)
  public void testPollRequestBuilderFailsWithInvalidLogIndex() {
    PollRequest.builder()
      .withId("test")
      .withUri("foo")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(-1L)
      .withLogTerm(1L)
      .build();
  }

  /**
   * Tests that the poll request builder fails with an invalid log term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPollRequestBuilderFailsWithInvalidLogTerm() {
    PollRequest.builder()
      .withId("test")
      .withUri("foo")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(-1L)
      .build();
  }

  /**
   * Tests that the poll request builder succeeds with a null log index and term.
   */
  public void testPollRequestBuilderSucceedsWithNullLogIndexAndNullLogTerm() {
    PollRequest.builder()
      .withId("test")
      .withUri("foo")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(null)
      .withLogTerm(null)
      .build();
  }

  /**
   * Tests that the poll request builder fails with a valid log index and null log term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPollRequestBuilderFailsWithValidLogIndexAndNullLogTerm() {
    PollRequest.builder()
      .withId("test")
      .withUri("foo")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(null)
      .build();
  }

  /**
   * Tests that the poll request builder fails with a null log index and valid log term.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testPollRequestBuilderFailsWithNullLogIndexAndValidLogTerm() {
    PollRequest.builder()
      .withId("test")
      .withUri("foo")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(null)
      .withLogTerm(1L)
      .build();
  }

  /**
   * Tests that the poll request builder succeeds when properly configured.
   */
  public void testPollRequestBuilderSucceedsWithValidConfiguration() {
    PollRequest request = PollRequest.builder()
      .withId("test")
      .withUri("foo")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .build();
    assertEquals(request.id(), "test");
    assertEquals(request.uri(), "foo");
    assertEquals(request.candidate(), "bar");
    assertEquals(request.term(), 1);
    assertEquals(request.logIndex().longValue(), 5);
    assertEquals(request.logTerm().longValue(), 1);
  }

  /**
   * Tests that the poll response builder fails without being properly configured.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollResponseBuilderFailsWithoutConfiguration() {
    PollResponse.builder().build();
  }

  /**
   * Tests that the poll response builder fails without an ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollResponseBuilderFailsWithoutId() {
    PollResponse.builder()
      .withUri("foo")
      .withTerm(1L)
      .withVoted(true)
      .build();
  }

  /**
   * Tests that the poll response builder fails with a null ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollResponseBuilderFailsWithNullId() {
    PollResponse.builder()
      .withId(null)
      .withUri("foo")
      .withTerm(1L)
      .withVoted(true)
      .build();
  }

  /**
   * Tests that the poll response builder fails without a member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollResponseBuilderFailsWithoutMember() {
    PollResponse.builder()
      .withId("test")
      .withTerm(1L)
      .withVoted(true)
      .build();
  }

  /**
   * Tests that the poll response builder fails with a null member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollResponseBuilderFailsWithNullMember() {
    PollResponse.builder()
      .withId("test")
      .withUri(null)
      .withTerm(1L)
      .withVoted(true)
      .build();
  }

  /**
   * Tests that the poll response builder fails with an invalid term.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollResponseBuilderFailsWithInvalidTerm() {
    PollResponse.builder()
      .withId("test")
      .withUri(null)
      .withTerm(-1L)
      .withVoted(true)
      .build();
  }

  /**
   * Tests that the poll response builder succeeds with a valid configuration.
   */
  public void testPollResponseBuilderSucceedsWithValidConfiguration() {
    PollResponse response = PollResponse.builder()
      .withId("test")
      .withUri("foo")
      .withTerm(1L)
      .withVoted(true)
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.uri(), "foo");
    assertEquals(response.term(), 1);
    assertTrue(response.voted());
  }

  /**
   * Tests that the query request builder when not configured.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryRequestBuilderFailsWithoutConfiguration() {
    QueryRequest.builder().build();
  }

  /**
   * Tests that the query request builder fails without a configured ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryRequestBuilderFailsWithoutId() {
    QueryRequest.builder().withUri("foo").withEntry(ByteBuffer.wrap("Hello world!".getBytes())).build();
  }

  /**
   * Tests that the query request builder fails with a null ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryRequestBuilderFailsWithNullId() {
    QueryRequest.builder().withId(null).build();
  }

  /**
   * Tests that the query request builder fails without a configured member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryRequestBuilderFailsWithoutMember() {
    QueryRequest.builder().withId("test").withEntry(ByteBuffer.wrap("Hello world!".getBytes())).build();
  }

  /**
   * Tests that the query request builder fails with a null member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryRequestBuilderFailsWithNullMember() {
    QueryRequest.builder().withUri(null).build();
  }

  /**
   * Tests that the query request builder fails without a configured entry.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryRequestBuilderFailsWithoutEntry() {
    QueryRequest.builder().withId("test").withUri("foo").build();
  }

  /**
   * Tests that the query request builder fails when no entry has been set.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryRequestBuilderEntrySetterFailsWithNullEntry() {
    QueryRequest.builder().withEntry(null).build();
  }

  /**
   * Tests that the query request builder fails when a null consistency is provided.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryRequestBuilderEntrySetterFailsWithNullConsistency() {
    QueryRequest.builder().withConsistency(null).build();
  }

  /**
   * Tests that the query request builder succeeds when properly configured.
   */
  public void testQueryRequestBuilderSucceedsWithValidConfiguration() {
    QueryRequest request = QueryRequest.builder()
      .withId("test")
      .withUri("foo")
      .withEntry(ByteBuffer.wrap("Hello world!".getBytes()))
      .withConsistency(Consistency.STRONG)
      .build();
    assertEquals(request.id(), "test");
    assertEquals(request.uri(), "foo");
    assertEquals(new String(request.entry().array()), "Hello world!");
    assertEquals(request.consistency(), Consistency.STRONG);
  }

  /**
   * Tests that the query response builder fails without being properly configured.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryResponseBuilderFailsWithoutConfiguration() {
    QueryResponse.builder().build();
  }

  /**
   * Tests that the query response builder fails without an ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryResponseBuilderFailsWithoutId() {
    QueryResponse.builder().withUri("foo").withResult("Hello world!").build();
  }

  /**
   * Tests that the query response builder fails with a null ID.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryResponseBuilderFailsWithNullId() {
    QueryResponse.builder().withId(null).build();
  }

  /**
   * Tests that the query response builder fails without a member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryResponseBuilderFailsWithoutMember() {
    QueryResponse.builder().withId("test").withResult("Hello world!").build();
  }

  /**
   * Tests that the query response builder fails with a null member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryResponseBuilderFailsWithNullMember() {
    QueryResponse.builder().withUri(null).build();
  }

  /**
   * Tests that the query response builder succeeds with a null result.
   */
  public void testQueryResponseBuilderSucceedsWithNullResult() {
    QueryResponse response = QueryResponse.builder()
      .withId("test")
      .withUri("foo")
      .withResult(null)
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.uri(), "foo");
    assertNull(response.result());
  }

  /**
   * Tests that the query response builder succeeds with a valid configuration.
   */
  public void testQueryResponseBuilderSucceedsWithValidConfiguration() {
    QueryResponse response = QueryResponse.builder()
      .withId("test")
      .withUri("foo")
      .withResult("Hello world!")
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.uri(), "foo");
    assertEquals(response.result(), "Hello world!");
  }

}
