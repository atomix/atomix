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

import io.atomix.primitive.session.SessionId;
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
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class RaftProxySequencerTest {

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventBeforeCommand() throws Throwable {
    RaftProxySequencer sequencer = new RaftProxySequencer(new RaftProxyState("test", SessionId.from(1), UUID.randomUUID().toString(), new TestPrimitiveType(), 1000));
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
    sequencer.sequenceEvent(request, () -> assertEquals(run.getAndIncrement(), 0));
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(run.getAndIncrement(), 1));
    assertEquals(run.get(), 2);
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAfterCommand() throws Throwable {
    RaftProxySequencer sequencer = new RaftProxySequencer(new RaftProxyState("test", SessionId.from(1), UUID.randomUUID().toString(), new TestPrimitiveType(), 1000));
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
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(run.getAndIncrement(), 0));
    sequencer.sequenceEvent(request, () -> assertEquals(run.getAndIncrement(), 1));
    assertEquals(run.get(), 2);
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAtCommand() throws Throwable {
    RaftProxySequencer sequencer = new RaftProxySequencer(new RaftProxyState("test", SessionId.from(1), UUID.randomUUID().toString(), new TestPrimitiveType(), 1000));
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
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(run.getAndIncrement(), 1));
    sequencer.sequenceEvent(request, () -> assertEquals(run.getAndIncrement(), 0));
    assertEquals(run.get(), 2);
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAfterAllCommands() throws Throwable {
    RaftProxySequencer sequencer = new RaftProxySequencer(new RaftProxyState("test", SessionId.from(1), UUID.randomUUID().toString(), new TestPrimitiveType(), 1000));
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
    sequencer.sequenceEvent(request1, () -> assertEquals(run.getAndIncrement(), 0));
    sequencer.sequenceEvent(request2, () -> assertEquals(run.getAndIncrement(), 2));
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(run.getAndIncrement(), 1));
    assertEquals(run.get(), 3);
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAbsentCommand() throws Throwable {
    RaftProxySequencer sequencer = new RaftProxySequencer(new RaftProxyState("test", SessionId.from(1), UUID.randomUUID().toString(), new TestPrimitiveType(), 1000));

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
    sequencer.sequenceEvent(request1, () -> assertEquals(run.getAndIncrement(), 0));
    sequencer.sequenceEvent(request2, () -> assertEquals(run.getAndIncrement(), 1));
    assertEquals(run.get(), 2);
  }

  /**
   * Tests sequencing callbacks with the sequencer.
   */
  @Test
  public void testSequenceResponses() throws Throwable {
    RaftProxySequencer sequencer = new RaftProxySequencer(new RaftProxyState("test", SessionId.from(1), UUID.randomUUID().toString(), new TestPrimitiveType(), 1000));
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
