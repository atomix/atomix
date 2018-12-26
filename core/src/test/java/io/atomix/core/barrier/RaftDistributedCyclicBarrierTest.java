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
package io.atomix.core.barrier;

import io.atomix.core.Atomix;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Raft distributed cyclic barrier test.
 */
public class RaftDistributedCyclicBarrierTest extends DistributedCyclicBarrierTest {
  @Override
  protected ProxyProtocol protocol() {
    return MultiRaftProtocol.builder()
        .withMinTimeout(Duration.ofSeconds(1))
        .withMaxTimeout(Duration.ofSeconds(1))
        .withMaxRetries(5)
        .build();
  }

  @Test
  public void testDelete() throws Exception {
    Atomix client = atomix();

    DistributedCyclicBarrier barrier;
    barrier = atomix().cyclicBarrierBuilder("test-delete")
        .withProtocol(protocol())
        .build();

    int count = client.getPrimitives(barrier.type()).size();
    barrier.delete();
    assertEquals(count - 1, client.getPrimitives(barrier.type()).size());

    try {
      barrier.getParties();
      fail();
    } catch (PrimitiveException.ClosedSession e) {
    }

    barrier = atomix().cyclicBarrierBuilder("test-delete")
        .withProtocol(protocol())
        .build();
    assertEquals(count, client.getPrimitives(barrier.type()).size());
  }
}
