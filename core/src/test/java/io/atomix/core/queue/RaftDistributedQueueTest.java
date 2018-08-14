/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.queue;

import io.atomix.core.Atomix;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Raft distributed queue test.
 */
public class RaftDistributedQueueTest extends DistributedQueueTest {
  @Override
  protected ProxyProtocol protocol() {
    return MultiRaftProtocol.builder()
        .withReadConsistency(ReadConsistency.LINEARIZABLE)
        .withMaxRetries(5)
        .build();
  }

  @Test
  public void testDelete() throws Exception {
    Atomix client = atomix();

    DistributedQueue<String> queue;
    queue = atomix().<String>queueBuilder("test-delete")
        .withProtocol(protocol())
        .build();
    assertFalse(client.getPrimitives(queue.type()).isEmpty());
    queue.delete();
    assertTrue(client.getPrimitives(queue.type()).isEmpty());

    try {
      queue.poll();
      fail();
    } catch (PrimitiveException.ClosedSession e) {
    }

    queue = atomix().<String>queueBuilder("test-delete")
        .withProtocol(protocol())
        .build();
    assertFalse(client.getPrimitives(queue.type()).isEmpty());
  }
}
