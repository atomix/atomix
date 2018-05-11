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
package io.atomix.core.semaphore.impl;

import io.atomix.core.Atomix;
import io.atomix.core.semaphore.AsyncDistributedSemaphore;
import io.atomix.core.semaphore.QueueStatus;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.utils.time.Version;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

public class RaftSemaphoreTest extends SemaphoreTest {
  @Override
  protected PrimitiveProtocol protocol() {
    return MultiRaftProtocol.builder()
            .withReadConsistency(ReadConsistency.LINEARIZABLE)
            .withMaxRetries(5)
            .build();
  }

  // Needs linearizable read consistency
  @Test(timeout = 10000)
  public void testQueueStatus() throws Exception {
    Atomix atomix = atomix();
    AsyncDistributedSemaphore semaphore = atomix.semaphoreBuilder("test-semaphore-status", protocol())
            .withInitialCapacity(10)
            .build()
            .async();

    semaphore.acquire(5).join();

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

    semaphore.release().join();
    acquire6.join();

    QueueStatus status4 = semaphore.queueStatus().get();
    assertEquals(1, status4.queueLength());
    assertEquals(10, status4.totalPermits());

    semaphore.release(10).join();
    acquire10.join();

    QueueStatus status5 = semaphore.queueStatus().get();
    assertEquals(0, status5.queueLength());
    assertEquals(0, status5.totalPermits());
  }
}
