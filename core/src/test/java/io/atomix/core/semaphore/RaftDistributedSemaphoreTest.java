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
package io.atomix.core.semaphore;

import io.atomix.core.Atomix;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RaftDistributedSemaphoreTest extends DistributedSemaphoreTest {
  @Override
  protected ProxyProtocol protocol() {
    return MultiRaftProtocol.builder("raft")
        .withReadConsistency(ReadConsistency.LINEARIZABLE)
        .withMaxRetries(5)
        .build();
  }

  @Test
  public void testDelete() throws Exception {
    Atomix client = atomix();

    DistributedSemaphore semaphore;
    semaphore = atomix().semaphoreBuilder("test-" + protocol().group() + "-semaphore-delete")
        .withProtocol(protocol())
        .build();

    int count = client.getPrimitives(semaphore.type()).size();
    semaphore.delete();
    assertEquals(count - 1, client.getPrimitives(semaphore.type()).size());

    try {
      semaphore.availablePermits();
      fail();
    } catch (PrimitiveException.ClosedSession e) {
    }

    semaphore = atomix().semaphoreBuilder("test-" + protocol().group() + "-semaphore-delete")
        .withProtocol(protocol())
        .build();
    assertEquals(count, client.getPrimitives(semaphore.type()).size());
  }
}
