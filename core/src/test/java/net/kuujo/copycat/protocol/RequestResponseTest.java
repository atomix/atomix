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
  public void testCommitRequestBuilderFailsWithoutConfiguration() {
    try {
      CommitRequest.builder().build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit request builder fails without a configured ID.
   */
  public void testCommitRequestBuilderFailsWithoutId() {
    try {
      CommitRequest.builder().withMember("foo").withEntry(ByteBuffer.wrap("Hello world!".getBytes())).build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit request builder fails with a null ID.
   */
  public void testCommitRequestBuilderFailsWithNullId() {
    try {
      CommitRequest.builder().withId(null).build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit request builder fails without a configured member.
   */
  public void testCommitRequestBuilderFailsWithoutMember() {
    try {
      CommitRequest.builder().withId("test").withEntry(ByteBuffer.wrap("Hello world!".getBytes())).build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit request builder fails with a null member.
   */
  public void testCommitRequestBuilderFailsWithNullMember() {
    try {
      CommitRequest.builder().withMember(null).build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit request builder fails without a configured entry.
   */
  public void testCommitRequestBuilderFailsWithoutEntry() {
    try {
      CommitRequest.builder().withId("test").withMember("foo").build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit request builder fails when no entry has been set.
   */
  public void testCommitRequestBuilderEntrySetterFailsWithNullEntry() {
    try {
      CommitRequest.builder().withEntry(null).build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit request builder succeeds when properly configured.
   */
  public void testCommitRequestBuilderSucceedsWithValidConfiguration() {
    CommitRequest request = CommitRequest.builder()
      .withId("test")
      .withMember("foo")
      .withEntry(ByteBuffer.wrap("Hello world!".getBytes())).build();
    assertEquals(request.id(), "test");
    assertEquals(request.member(), "foo");
    assertEquals(new String(request.entry().array()), "Hello world!");
  }

  /**
   * Tests that the commit response builder fails without being properly configured.
   */
  public void testCommitResponseBuilderFailsWithoutConfiguration() {
    try {
      CommitResponse.builder().build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit response builder fails without an ID.
   */
  public void testCommitResponseBuilderFailsWithoutId() {
    try {
      CommitResponse.builder().withMember("foo").withResult("Hello world!").build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit response builder fails with a null ID.
   */
  public void testCommitResponseBuilderFailsWithNullId() {
    try {
      CommitResponse.builder().withId(null).build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit response builder fails without a member.
   */
  public void testCommitResponseBuilderFailsWithoutMember() {
    try {
      CommitResponse.builder().withId("test").withResult("Hello world!").build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit response builder fails with a null member.
   */
  public void testCommitResponseBuilderFailsWithNullMember() {
    try {
      CommitResponse.builder().withMember(null).build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the commit response builder succeeds with a null result.
   */
  public void testCommitResponseBuilderSucceedsWithNullResult() {
    CommitResponse response = CommitResponse.builder()
      .withId("test")
      .withMember("foo")
      .withResult(null)
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.member(), "foo");
    assertNull(response.result());
  }

  /**
   * Tests that the commit response builder succeeds with a valid configuration.
   */
  public void testCommitResponseBuilderSucceedsWithValidConfiguration() {
    CommitResponse response = CommitResponse.builder()
      .withId("test")
      .withMember("foo")
      .withResult("Hello world!")
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.member(), "foo");
    assertEquals(response.result(), "Hello world!");
  }

  /**
   * Tests that the append request builder when not configured.
   */
  public void testAppendRequestBuilderFailsWithoutConfiguration() {
    try {
      AppendRequest.builder().build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append request builder fails without a configured ID.
   */
  public void testAppendRequestBuilderFailsWithoutId() {
    try {
      AppendRequest.builder()
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append request builder fails with a null ID.
   */
  public void testAppendRequestBuilderFailsWithNullId() {
    try {
      AppendRequest.builder()
        .withId(null)
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append request builder fails without a configured member.
   */
  public void testAppendRequestBuilderFailsWithoutMember() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withLeader("bar")
        .withTerm(1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append request builder fails with a null member.
   */
  public void testAppendRequestBuilderFailsWithNullMember() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember(null)
        .withLeader("bar")
        .withTerm(1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append request builder fails without a leader.
   */
  public void testAppendRequestBuilderFailsWithoutLeader() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember("foo")
        .withTerm(1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append request builder fails with a null leader.
   */
  public void testAppendRequestBuilderFailsWithNullLeader() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader(null)
        .withTerm(1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append request builder fails without a term.
   */
  public void testAppendRequestBuilderFailsWithoutTerm() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the append request builder fails without an invalid term.
   */
  public void testAppendRequestBuilderFailsWithInvalidTerm() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(-1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the append request builder fails without entries.
   */
  public void testAppendRequestBuilderFailsWithoutEntries() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append request builder fails without null entries.
   */
  public void testAppendRequestBuilderFailsWithNullEntries() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withEntries((List<ByteBuffer>) null)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append request builder fails with an invalid log index.
   */
  public void testAppendRequestBuilderFailsWithInvalidLogIndex() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(-1L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IndexOutOfBoundsException e) {
    }
  }

  /**
   * Tests that the append request builder fails with an invalid log term.
   */
  public void testAppendRequestBuilderFailsWithInvalidLogTerm() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(5L)
        .withLogTerm(-1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the append request builder succeeds with a null log index and term.
   */
  public void testAppendRequestBuilderSucceedsWithNullLogIndexAndNullLogTerm() {
    AppendRequest.builder()
      .withId("test")
      .withMember("foo")
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
  public void testAppendRequestBuilderFailsWithValidLogIndexAndNullLogTerm() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(5L)
        .withLogTerm(null)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the append request builder fails with a null log index and valid log term.
   */
  public void testAppendRequestBuilderFailsWithNullLogIndexAndValidLogTerm() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(null)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the append request builder succeeds with a null commit index.
   */
  public void testAppendRequestBuilderSucceedsWithNullCommitIndex() {
    AppendRequest.builder()
      .withId("test")
      .withMember("foo")
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
  public void testAppendRequestBuilderFailsWithInvalidCommitIndex() {
    try {
      AppendRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(-1L)
        .build();
      fail();
    } catch (IndexOutOfBoundsException e) {
    }
  }

  /**
   * Tests that the append request builder succeeds when properly configured.
   */
  public void testAppendRequestBuilderSucceedsWithValidConfiguration() {
    AppendRequest request = AppendRequest.builder()
      .withId("test")
      .withMember("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
    assertEquals(request.id(), "test");
    assertEquals(request.member(), "foo");
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
  public void testAppendResponseBuilderFailsWithoutConfiguration() {
    try {
      AppendResponse.builder().build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append response builder fails without an ID.
   */
  public void testAppendResponseBuilderFailsWithoutId() {
    try {
      AppendResponse.builder()
        .withMember("foo")
        .withTerm(1L)
        .withSucceeded(true)
        .withLogIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append response builder fails with a null ID.
   */
  public void testAppendResponseBuilderFailsWithNullId() {
    try {
      AppendResponse.builder()
        .withId(null)
        .withMember("foo")
        .withTerm(1L)
        .withSucceeded(true)
        .withLogIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append response builder fails without a member.
   */
  public void testAppendResponseBuilderFailsWithoutMember() {
    try {
      AppendResponse.builder()
        .withId("test")
        .withTerm(1L)
        .withSucceeded(true)
        .withLogIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append response builder fails with a null member.
   */
  public void testAppendResponseBuilderFailsWithNullMember() {
    try {
      AppendResponse.builder()
        .withId("test")
        .withMember(null)
        .withTerm(1L)
        .withSucceeded(true)
        .withLogIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append response builder fails with an invalid term.
   */
  public void testAppendResponseBuilderFailsWithInvalidTerm() {
    try {
      AppendResponse.builder()
        .withId("test")
        .withMember(null)
        .withTerm(-1L)
        .withSucceeded(true)
        .withLogIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append response builder succeeds with a null index.
   */
  public void testAppendResponseBuilderSucceedsWithNullIndex() {
    AppendResponse.builder()
      .withId("test")
      .withMember("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(null)
      .build();
  }

  /**
   * Tests that the append response builder fails with an invalid index.
   */
  public void testAppendResponseBuilderFailsWithInvalidIndex() {
    try {
      AppendResponse.builder()
        .withId("test")
        .withMember(null)
        .withTerm(1L)
        .withSucceeded(true)
        .withLogIndex(-1L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the append response builder succeeds with a valid configuration.
   */
  public void testAppendResponseBuilderSucceedsWithValidConfiguration() {
    AppendResponse response = AppendResponse.builder()
      .withId("test")
      .withMember("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.member(), "foo");
    assertEquals(response.term(), 1);
    assertTrue(response.succeeded());
    assertEquals(response.logIndex().longValue(), 4);
  }

  /**
   * Tests that the ping request builder when not configured.
   */
  public void testPingRequestBuilderFailsWithoutConfiguration() {
    try {
      PingRequest.builder().build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping request builder fails without a configured ID.
   */
  public void testPingRequestBuilderFailsWithoutId() {
    try {
      PingRequest.builder()
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping request builder fails with a null ID.
   */
  public void testPingRequestBuilderFailsWithNullId() {
    try {
      PingRequest.builder()
        .withId(null)
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping request builder fails without a configured member.
   */
  public void testPingRequestBuilderFailsWithoutMember() {
    try {
      PingRequest.builder()
        .withId("test")
        .withLeader("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping request builder fails with a null member.
   */
  public void testPingRequestBuilderFailsWithNullMember() {
    try {
      PingRequest.builder()
        .withId("test")
        .withMember(null)
        .withLeader("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping request builder fails without a leader.
   */
  public void testPingRequestBuilderFailsWithoutLeader() {
    try {
      PingRequest.builder()
        .withId("test")
        .withMember("foo")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping request builder fails with a null leader.
   */
  public void testPingRequestBuilderFailsWithNullLeader() {
    try {
      PingRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader(null)
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping request builder fails without a term.
   */
  public void testPingRequestBuilderFailsWithoutTerm() {
    try {
      PingRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the ping request builder fails without an invalid term.
   */
  public void testPingRequestBuilderFailsWithInvalidTerm() {
    try {
      PingRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(-1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the ping request builder fails with an invalid log index.
   */
  public void testPingRequestBuilderFailsWithInvalidLogIndex() {
    try {
      PingRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withLogIndex(-1L)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IndexOutOfBoundsException e) {
    }
  }

  /**
   * Tests that the ping request builder fails with an invalid log term.
   */
  public void testPingRequestBuilderFailsWithInvalidLogTerm() {
    try {
      PingRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(-1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the ping request builder succeeds with a null log index and term.
   */
  public void testPingRequestBuilderSucceedsWithNullLogIndexAndNullLogTerm() {
    PingRequest.builder()
      .withId("test")
      .withMember("foo")
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
  public void testPingRequestBuilderFailsWithValidLogIndexAndNullLogTerm() {
    try {
      PingRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(null)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the ping request builder fails with a null log index and valid log term.
   */
  public void testPingRequestBuilderFailsWithNullLogIndexAndValidLogTerm() {
    try {
      PingRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withLogIndex(null)
        .withLogTerm(1L)
        .withCommitIndex(4L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the ping request builder succeeds with a null commit index.
   */
  public void testPingRequestBuilderSucceedsWithNullCommitIndex() {
    PingRequest.builder()
      .withId("test")
      .withMember("foo")
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
  public void testPingRequestBuilderFailsWithInvalidCommitIndex() {
    try {
      PingRequest.builder()
        .withId("test")
        .withMember("foo")
        .withLeader("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .withCommitIndex(-1L)
        .build();
      fail();
    } catch (IndexOutOfBoundsException e) {
    }
  }

  /**
   * Tests that the ping request builder succeeds when properly configured.
   */
  public void testPingRequestBuilderSucceedsWithValidConfiguration() {
    PingRequest request = PingRequest.builder()
      .withId("test")
      .withMember("foo")
      .withLeader("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
    assertEquals(request.id(), "test");
    assertEquals(request.member(), "foo");
    assertEquals(request.leader(), "bar");
    assertEquals(request.term(), 1);
    assertEquals(request.logIndex().longValue(), 5);
    assertEquals(request.logTerm().longValue(), 1);
    assertEquals(request.commitIndex().longValue(), 4);
  }

  /**
   * Tests that the ping response builder fails without being properly configured.
   */
  public void testPingResponseBuilderFailsWithoutConfiguration() {
    try {
      PingResponse.builder().build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping response builder fails without an ID.
   */
  public void testPingResponseBuilderFailsWithoutId() {
    try {
      PingResponse.builder()
        .withMember("foo")
        .withTerm(1L)
        .withSucceeded(true)
        .withLogIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping response builder fails with a null ID.
   */
  public void testPingResponseBuilderFailsWithNullId() {
    try {
      PingResponse.builder()
        .withId(null)
        .withMember("foo")
        .withTerm(1L)
        .withSucceeded(true)
        .withLogIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping response builder fails without a member.
   */
  public void testPingResponseBuilderFailsWithoutMember() {
    try {
      PingResponse.builder()
        .withId("test")
        .withTerm(1L)
        .withSucceeded(true)
        .withLogIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping response builder fails with a null member.
   */
  public void testPingResponseBuilderFailsWithNullMember() {
    try {
      PingResponse.builder()
        .withId("test")
        .withMember(null)
        .withTerm(1L)
        .withSucceeded(true)
        .withLogIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping response builder fails with an invalid term.
   */
  public void testPingResponseBuilderFailsWithInvalidTerm() {
    try {
      PingResponse.builder()
        .withId("test")
        .withMember(null)
        .withTerm(-1L)
        .withSucceeded(true)
        .withLogIndex(4L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping response builder succeeds with a null index.
   */
  public void testPingResponseBuilderSucceedsWithNullIndex() {
    PingResponse.builder()
      .withId("test")
      .withMember("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(null)
      .build();
  }

  /**
   * Tests that the ping response builder fails with an invalid index.
   */
  public void testPingResponseBuilderFailsWithInvalidIndex() {
    try {
      PingResponse.builder()
        .withId("test")
        .withMember(null)
        .withTerm(1L)
        .withSucceeded(true)
        .withLogIndex(-1L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the ping response builder succeeds with a valid configuration.
   */
  public void testPingResponseBuilderSucceedsWithValidConfiguration() {
    PingResponse response = PingResponse.builder()
      .withId("test")
      .withMember("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.member(), "foo");
    assertEquals(response.term(), 1);
    assertTrue(response.succeeded());
    assertEquals(response.logIndex().longValue(), 4);
  }

  /**
   * Tests that the poll request builder when not configured.
   */
  public void testPollRequestBuilderFailsWithoutConfiguration() {
    try {
      PollRequest.builder().build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll request builder fails without a configured ID.
   */
  public void testPollRequestBuilderFailsWithoutId() {
    try {
      PollRequest.builder()
        .withMember("foo")
        .withCandidate("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll request builder fails with a null ID.
   */
  public void testPollRequestBuilderFailsWithNullId() {
    try {
      PollRequest.builder()
        .withId(null)
        .withMember("foo")
        .withCandidate("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll request builder fails without a configured member.
   */
  public void testPollRequestBuilderFailsWithoutMember() {
    try {
      PollRequest.builder()
        .withId("test")
        .withCandidate("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll request builder fails with a null member.
   */
  public void testPollRequestBuilderFailsWithNullMember() {
    try {
      PollRequest.builder()
        .withId("test")
        .withMember(null)
        .withCandidate("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll request builder fails without a candidate.
   */
  public void testPollRequestBuilderFailsWithoutCandidate() {
    try {
      PollRequest.builder()
        .withId("test")
        .withMember("foo")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll request builder fails with a null candidate.
   */
  public void testPollRequestBuilderFailsWithNullCandidate() {
    try {
      PollRequest.builder()
        .withId("test")
        .withMember("foo")
        .withCandidate(null)
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll request builder fails without a term.
   */
  public void testPollRequestBuilderFailsWithoutTerm() {
    try {
      PollRequest.builder()
        .withId("test")
        .withMember("foo")
        .withCandidate("bar")
        .withLogIndex(5L)
        .withLogTerm(1L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the poll request builder fails without an invalid term.
   */
  public void testPollRequestBuilderFailsWithInvalidTerm() {
    try {
      PollRequest.builder()
        .withId("test")
        .withMember("foo")
        .withCandidate("bar")
        .withTerm(-1)
        .withLogIndex(5L)
        .withLogTerm(1L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the poll request builder fails with an invalid log index.
   */
  public void testPollRequestBuilderFailsWithInvalidLogIndex() {
    try {
      PollRequest.builder()
        .withId("test")
        .withMember("foo")
        .withCandidate("bar")
        .withTerm(1)
        .withLogIndex(-1L)
        .withLogTerm(1L)
        .build();
      fail();
    } catch (IndexOutOfBoundsException e) {
    }
  }

  /**
   * Tests that the poll request builder fails with an invalid log term.
   */
  public void testPollRequestBuilderFailsWithInvalidLogTerm() {
    try {
      PollRequest.builder()
        .withId("test")
        .withMember("foo")
        .withCandidate("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(-1L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the poll request builder succeeds with a null log index and term.
   */
  public void testPollRequestBuilderSucceedsWithNullLogIndexAndNullLogTerm() {
    PollRequest.builder()
      .withId("test")
      .withMember("foo")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(null)
      .withLogTerm(null)
      .build();
  }

  /**
   * Tests that the poll request builder fails with a valid log index and null log term.
   */
  public void testPollRequestBuilderFailsWithValidLogIndexAndNullLogTerm() {
    try {
      PollRequest.builder()
        .withId("test")
        .withMember("foo")
        .withCandidate("bar")
        .withTerm(1)
        .withLogIndex(5L)
        .withLogTerm(null)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the poll request builder fails with a null log index and valid log term.
   */
  public void testPollRequestBuilderFailsWithNullLogIndexAndValidLogTerm() {
    try {
      PollRequest.builder()
        .withId("test")
        .withMember("foo")
        .withCandidate("bar")
        .withTerm(1)
        .withLogIndex(null)
        .withLogTerm(1L)
        .build();
      fail();
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Tests that the poll request builder succeeds when properly configured.
   */
  public void testPollRequestBuilderSucceedsWithValidConfiguration() {
    PollRequest request = PollRequest.builder()
      .withId("test")
      .withMember("foo")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .build();
    assertEquals(request.id(), "test");
    assertEquals(request.member(), "foo");
    assertEquals(request.candidate(), "bar");
    assertEquals(request.term(), 1);
    assertEquals(request.logIndex().longValue(), 5);
    assertEquals(request.logTerm().longValue(), 1);
  }

  /**
   * Tests that the poll response builder fails without being properly configured.
   */
  public void testPollResponseBuilderFailsWithoutConfiguration() {
    try {
      PollResponse.builder().build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll response builder fails without an ID.
   */
  public void testPollResponseBuilderFailsWithoutId() {
    try {
      PollResponse.builder()
        .withMember("foo")
        .withTerm(1L)
        .withVoted(true)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll response builder fails with a null ID.
   */
  public void testPollResponseBuilderFailsWithNullId() {
    try {
      PollResponse.builder()
        .withId(null)
        .withMember("foo")
        .withTerm(1L)
        .withVoted(true)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll response builder fails without a member.
   */
  public void testPollResponseBuilderFailsWithoutMember() {
    try {
      PollResponse.builder()
        .withId("test")
        .withTerm(1L)
        .withVoted(true)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll response builder fails with a null member.
   */
  public void testPollResponseBuilderFailsWithNullMember() {
    try {
      PollResponse.builder()
        .withId("test")
        .withMember(null)
        .withTerm(1L)
        .withVoted(true)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll response builder fails with an invalid term.
   */
  public void testPollResponseBuilderFailsWithInvalidTerm() {
    try {
      PollResponse.builder()
        .withId("test")
        .withMember(null)
        .withTerm(-1L)
        .withVoted(true)
        .build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the poll response builder succeeds with a valid configuration.
   */
  public void testPollResponseBuilderSucceedsWithValidConfiguration() {
    PollResponse response = PollResponse.builder()
      .withId("test")
      .withMember("foo")
      .withTerm(1L)
      .withVoted(true)
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.member(), "foo");
    assertEquals(response.term(), 1);
    assertTrue(response.voted());
  }

  /**
   * Tests that the sync request builder when not configured.
   */
  public void testSyncRequestBuilderFailsWithoutConfiguration() {
    try {
      SyncRequest.builder().build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the sync request builder fails without a configured ID.
   */
  public void testSyncRequestBuilderFailsWithoutId() {
    try {
      SyncRequest.builder().withMember("foo").build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the sync request builder fails with a null ID.
   */
  public void testSyncRequestBuilderFailsWithNullId() {
    try {
      SyncRequest.builder().withId(null).build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the sync request builder fails without a configured member.
   */
  public void testSyncRequestBuilderFailsWithoutMember() {
    try {
      SyncRequest.builder().withId("test").build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the sync request builder fails with a null member.
   */
  public void testSyncRequestBuilderFailsWithNullMember() {
    try {
      SyncRequest.builder().withMember(null).build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the sync request builder succeeds when properly configured.
   */
  public void testSyncRequestBuilderSucceedsWithValidConfiguration() {
    SyncRequest request = SyncRequest.builder()
      .withId("test")
      .withMember("foo")
      .build();
    assertEquals(request.id(), "test");
    assertEquals(request.member(), "foo");
  }

  /**
   * Tests that the sync response builder fails without being properly configured.
   */
  public void testSyncResponseBuilderFailsWithoutConfiguration() {
    try {
      SyncResponse.builder().build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the sync response builder fails without an ID.
   */
  public void testSyncResponseBuilderFailsWithoutId() {
    try {
      SyncResponse.builder().withMember("foo").build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the sync response builder fails with a null ID.
   */
  public void testSyncResponseBuilderFailsWithNullId() {
    try {
      SyncResponse.builder().withId(null).build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the sync response builder fails without a member.
   */
  public void testSyncResponseBuilderFailsWithoutMember() {
    try {
      SyncResponse.builder().withId("test").build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the sync response builder fails with a null member.
   */
  public void testSyncResponseBuilderFailsWithNullMember() {
    try {
      SyncResponse.builder().withMember(null).build();
      fail();
    } catch (NullPointerException e) {
    }
  }

  /**
   * Tests that the sync response builder succeeds with a valid configuration.
   */
  public void testSyncResponseBuilderSucceedsWithValidConfiguration() {
    SyncResponse response = SyncResponse.builder()
      .withId("test")
      .withMember("foo")
      .build();
    assertEquals(response.id(), "test");
    assertEquals(response.member(), "foo");
  }

}
