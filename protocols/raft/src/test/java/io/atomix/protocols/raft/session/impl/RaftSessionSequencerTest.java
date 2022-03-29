// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.session.impl;

import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.TestPrimitiveType;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.PublishRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftResponse;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Client sequencer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftSessionSequencerTest {

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventBeforeCommand() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));
    long sequence = sequencer.nextRequest();

    PublishRequest request = PublishRequest.builder()
        .withSession(1)
        .withEventIndex(1)
        .withPreviousIndex(0)
        .withEvents(Collections.emptyList())
        .build();

    CommandResponse response = CommandResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(2)
        .withEventIndex(1)
        .build();

    AtomicInteger run = new AtomicInteger();
    sequencer.sequenceEvent(request, () -> assertEquals(0, run.getAndIncrement()));
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(1, run.getAndIncrement()));
    assertEquals(2, run.get());
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAfterCommand() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));
    long sequence = sequencer.nextRequest();

    PublishRequest request = PublishRequest.builder()
        .withSession(1)
        .withEventIndex(1)
        .withPreviousIndex(0)
        .withEvents(Collections.emptyList())
        .build();

    CommandResponse response = CommandResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(2)
        .withEventIndex(1)
        .build();

    AtomicInteger run = new AtomicInteger();
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(0, run.getAndIncrement()));
    sequencer.sequenceEvent(request, () -> assertEquals(1, run.getAndIncrement()));
    assertEquals(2, run.get());
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAtCommand() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));
    long sequence = sequencer.nextRequest();

    PublishRequest request = PublishRequest.builder()
        .withSession(1)
        .withEventIndex(2)
        .withPreviousIndex(0)
        .withEvents(Collections.emptyList())
        .build();

    CommandResponse response = CommandResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(2)
        .withEventIndex(2)
        .build();

    AtomicInteger run = new AtomicInteger();
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(1, run.getAndIncrement()));
    sequencer.sequenceEvent(request, () -> assertEquals(0, run.getAndIncrement()));
    assertEquals(2, run.get());
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAfterAllCommands() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));
    long sequence = sequencer.nextRequest();

    PublishRequest request1 = PublishRequest.builder()
        .withSession(1)
        .withEventIndex(2)
        .withPreviousIndex(0)
        .withEvents(Collections.emptyList())
        .build();

    PublishRequest request2 = PublishRequest.builder()
        .withSession(1)
        .withEventIndex(3)
        .withPreviousIndex(2)
        .withEvents(Collections.emptyList())
        .build();

    CommandResponse response = CommandResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(2)
        .withEventIndex(2)
        .build();

    AtomicInteger run = new AtomicInteger();
    sequencer.sequenceEvent(request1, () -> assertEquals(0, run.getAndIncrement()));
    sequencer.sequenceEvent(request2, () -> assertEquals(2, run.getAndIncrement()));
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(1, run.getAndIncrement()));
    assertEquals(3, run.get());
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAbsentCommand() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));

    PublishRequest request1 = PublishRequest.builder()
        .withSession(1)
        .withEventIndex(2)
        .withPreviousIndex(0)
        .withEvents(Collections.emptyList())
        .build();

    PublishRequest request2 = PublishRequest.builder()
        .withSession(1)
        .withEventIndex(3)
        .withPreviousIndex(2)
        .withEvents(Collections.emptyList())
        .build();

    AtomicInteger run = new AtomicInteger();
    sequencer.sequenceEvent(request1, () -> assertEquals(0, run.getAndIncrement()));
    sequencer.sequenceEvent(request2, () -> assertEquals(1, run.getAndIncrement()));
    assertEquals(2, run.get());
  }

  /**
   * Tests sequencing callbacks with the sequencer.
   */
  @Test
  public void testSequenceResponses() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));
    long sequence1 = sequencer.nextRequest();
    long sequence2 = sequencer.nextRequest();
    assertTrue(sequence2 == sequence1 + 1);

    CommandResponse commandResponse = CommandResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(2)
        .withEventIndex(0)
        .build();

    QueryResponse queryResponse = QueryResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .withIndex(2)
        .withEventIndex(0)
        .build();

    AtomicBoolean run = new AtomicBoolean();
    sequencer.sequenceResponse(sequence2, queryResponse, () -> run.set(true));
    sequencer.sequenceResponse(sequence1, commandResponse, () -> assertFalse(run.get()));
    assertTrue(run.get());
  }
}
