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
import io.atomix.utils.time.Version;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RaftAtomicSemaphoreTest extends AtomicSemaphoreTest {
  @Override
  protected ProxyProtocol protocol() {
    return MultiRaftProtocol.builder("raft")
        .withReadConsistency(ReadConsistency.LINEARIZABLE)
        .withMaxRetries(5)
        .build();
  }

  // Needs linearizable read consistency
  @Test(timeout = 10000)
  public void testQueueStatus() throws Exception {
    Atomix atomix = atomix();
    AsyncAtomicSemaphore semaphore = atomix.atomicSemaphoreBuilder("test-" + protocol().group() + "-atomic-semaphore-status")
        .withProtocol(protocol())
        .withInitialCapacity(10)
        .build()
        .async();

    semaphore.acquire(5).get(30, TimeUnit.SECONDS);

    QueueStatus status = semaphore.queueStatus().get();
    assertEquals(0, status.queueLength());
    assertEquals(0, status.totalPermits());

    CompletableFuture<Version> acquire6 = semaphore.acquire(6);
    QueueStatus status2 = semaphore.queueStatus().get();
    assertEquals(1, status2.queueLength());
    assertEquals(6, status2.totalPermits());


    CompletableFuture<Version> acquire10 = semaphore.acquire(10);
    QueueStatus status3 = semaphore.queueStatus().get();
    assertEquals(2, status3.queueLength());
    assertEquals(16, status3.totalPermits());

    semaphore.release().get(30, TimeUnit.SECONDS);
    acquire6.get(30, TimeUnit.SECONDS);

    QueueStatus status4 = semaphore.queueStatus().get();
    assertEquals(1, status4.queueLength());
    assertEquals(10, status4.totalPermits());

    semaphore.release(10).get(30, TimeUnit.SECONDS);
    acquire10.get(30, TimeUnit.SECONDS);

    QueueStatus status5 = semaphore.queueStatus().get();
    assertEquals(0, status5.queueLength());
    assertEquals(0, status5.totalPermits());
  }

  @Test
  public void testDelete() throws Exception {
    Atomix client = atomix();

    AtomicSemaphore semaphore;
    semaphore = atomix().atomicSemaphoreBuilder("test-" + protocol().group() + "-atomic-semaphore-delete")
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

    semaphore = atomix().atomicSemaphoreBuilder("test-" + protocol().group() + "-atomic-semaphore-delete")
        .withProtocol(protocol())
        .build();
    assertEquals(count, client.getPrimitives(semaphore.type()).size());
  }
}
