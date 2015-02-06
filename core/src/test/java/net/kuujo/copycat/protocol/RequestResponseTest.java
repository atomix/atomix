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

import net.kuujo.copycat.protocol.rpc.*;
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
   * Tests that the commit request builder fails without a configured member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitRequestBuilderFailsWithoutMember() {
    CommitRequest.builder().withEntry(ByteBuffer.wrap("Hello world!".getBytes())).build();
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
    CommitRequest.builder().withUri("foo").build();
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
      .withUri("foo")
      .withEntry(ByteBuffer.wrap("Hello world!".getBytes())).build();
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
   * Tests that the commit response builder fails without a member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testCommitResponseBuilderFailsWithoutMember() {
    CommitResponse.builder().withResult(ByteBuffer.wrap("Hello world!".getBytes())).build();
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
      .withUri("foo")
      .withResult(null)
      .build();
    assertEquals(response.uri(), "foo");
    assertNull(response.result());
  }

  /**
   * Tests that the commit response builder succeeds with a valid configuration.
   */
  public void testCommitResponseBuilderSucceedsWithValidConfiguration() {
    CommitResponse response = CommitResponse.builder()
      .withUri("foo")
      .withResult(ByteBuffer.wrap("Hello world!".getBytes()))
      .build();
    assertEquals(response.uri(), "foo");
    assertEquals(new String(response.result().array()), "Hello world!");
  }

  /**
   * Tests that the append request builder when not configured.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRequestBuilderFailsWithoutConfiguration() {
    AppendRequest.builder().build();
  }

  /**
   * Tests that the append request builder fails without a configured member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendRequestBuilderFailsWithoutMember() {
    AppendRequest.builder()
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
      .withUri("foo")
      .withLeader("bar")
      .withTerm(1)
      .withEntries(ByteBuffer.wrap("Hello world!".getBytes()))
      .withLogIndex(5L)
      .withLogTerm(1L)
      .withCommitIndex(4L)
      .build();
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
   * Tests that the append response builder fails without a member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testAppendResponseBuilderFailsWithoutMember() {
    AppendResponse.builder()
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
      .withUri("foo")
      .withTerm(1L)
      .withSucceeded(true)
      .withLogIndex(4L)
      .build();
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
   * Tests that the poll request builder fails without a configured member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollRequestBuilderFailsWithoutMember() {
    PollRequest.builder()
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
      .withUri("foo")
      .withCandidate("bar")
      .withTerm(1)
      .withLogIndex(5L)
      .withLogTerm(1L)
      .build();
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
   * Tests that the poll response builder fails without a member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testPollResponseBuilderFailsWithoutMember() {
    PollResponse.builder()
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
      .withUri("foo")
      .withTerm(1L)
      .withVoted(true)
      .build();
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
   * Tests that the query request builder fails without a configured member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryRequestBuilderFailsWithoutMember() {
    QueryRequest.builder().withEntry(ByteBuffer.wrap("Hello world!".getBytes())).build();
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
    QueryRequest.builder().withUri("foo").build();
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
      .withUri("foo")
      .withEntry(ByteBuffer.wrap("Hello world!".getBytes()))
      .withConsistency(Consistency.STRONG)
      .build();
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
   * Tests that the query response builder fails without a member.
   */
  @Test(expectedExceptions = NullPointerException.class)
  public void testQueryResponseBuilderFailsWithoutMember() {
    QueryResponse.builder().withResult(ByteBuffer.wrap("Hello world!".getBytes())).build();
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
      .withUri("foo")
      .withResult(null)
      .build();
    assertEquals(response.uri(), "foo");
    assertNull(response.result());
  }

  /**
   * Tests that the query response builder succeeds with a valid configuration.
   */
  public void testQueryResponseBuilderSucceedsWithValidConfiguration() {
    QueryResponse response = QueryResponse.builder()
      .withUri("foo")
      .withResult(ByteBuffer.wrap("Hello world!".getBytes()))
      .build();
    assertEquals(response.uri(), "foo");
    assertEquals(new String(response.result().array()), "Hello world!");
  }

}
