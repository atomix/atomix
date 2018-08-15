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
package io.atomix.core;

import io.atomix.core.barrier.PrimaryBackupDistributedCyclicBarrierTest;
import io.atomix.core.barrier.RaftDistributedCyclicBarrierTest;
import io.atomix.core.counter.CrdtCounterTest;
import io.atomix.core.counter.PrimaryBackupAtomicCounterTest;
import io.atomix.core.counter.RaftAtomicCounterTest;
import io.atomix.core.election.PrimaryBackupLeaderElectionTest;
import io.atomix.core.election.PrimaryBackupLeaderElectorTest;
import io.atomix.core.election.RaftLeaderElectionTest;
import io.atomix.core.election.RaftLeaderElectorTest;
import io.atomix.core.idgenerator.PrimaryBackupIdGeneratorTest;
import io.atomix.core.idgenerator.RaftIdGeneratorTest;
import io.atomix.core.list.PrimaryBackupDistributedListTest;
import io.atomix.core.list.RaftDistributedListTest;
import io.atomix.core.lock.PrimaryBackupAtomicLockTest;
import io.atomix.core.lock.PrimaryBackupDistributedLockTest;
import io.atomix.core.lock.RaftAtomicLockTest;
import io.atomix.core.lock.RaftDistributedLockTest;
import io.atomix.core.map.PrimaryBackupAtomicCounterMapTest;
import io.atomix.core.map.PrimaryBackupAtomicMapTest;
import io.atomix.core.map.PrimaryBackupAtomicNavigableMapTest;
import io.atomix.core.map.PrimaryBackupDistributedMapTest;
import io.atomix.core.map.PrimaryBackupDistributedNavigableMapTest;
import io.atomix.core.map.RaftAtomicCounterMapTest;
import io.atomix.core.map.RaftAtomicMapTest;
import io.atomix.core.map.RaftAtomicNavigableMapTest;
import io.atomix.core.map.RaftDistributedMapTest;
import io.atomix.core.map.RaftDistributedNavigableMapTest;
import io.atomix.core.multimap.PrimaryBackupAtomicMultimapTest;
import io.atomix.core.multimap.PrimaryBackupDistributedMultimapTest;
import io.atomix.core.multimap.RaftAtomicMultimapTest;
import io.atomix.core.multimap.RaftDistributedMultimapTest;
import io.atomix.core.multiset.PrimaryBackupDistributedMultisetTest;
import io.atomix.core.multiset.RaftDistributedMultisetTest;
import io.atomix.core.queue.PrimaryBackupDistributedQueueTest;
import io.atomix.core.queue.RaftDistributedQueueTest;
import io.atomix.core.semaphore.PrimaryBackupAtomicSemaphoreTest;
import io.atomix.core.semaphore.PrimaryBackupDistributedSemaphoreTest;
import io.atomix.core.semaphore.RaftAtomicSemaphoreTest;
import io.atomix.core.semaphore.RaftDistributedSemaphoreTest;
import io.atomix.core.set.AntiEntropyDistributedSetTest;
import io.atomix.core.set.CrdtDistributedSetTest;
import io.atomix.core.set.PrimaryBackupDistributedNavigableSetTest;
import io.atomix.core.set.PrimaryBackupDistributedSetTest;
import io.atomix.core.set.RaftDistributedNavigableSetTest;
import io.atomix.core.set.RaftDistributedSetTest;
import io.atomix.core.transaction.PrimaryBackupTransactionalMapTest;
import io.atomix.core.transaction.PrimaryBackupTransactionalSetTest;
import io.atomix.core.transaction.RaftTransactionalMapTest;
import io.atomix.core.transaction.RaftTransactionalSetTest;
import io.atomix.core.tree.PrimaryBackupAtomicDocumentTreeTest;
import io.atomix.core.tree.RaftAtomicDocumentTreeTest;
import io.atomix.core.value.CrdtDistributedValueTest;
import io.atomix.core.value.PrimaryBackupAtomicValueTest;
import io.atomix.core.value.RaftAtomicValueTest;
import io.atomix.core.workqueue.PrimaryBackupWorkQueueTest;
import io.atomix.core.workqueue.RaftWorkQueueTest;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Primitives test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    PrimaryBackupDistributedCyclicBarrierTest.class,
    RaftDistributedCyclicBarrierTest.class,
    CrdtCounterTest.class,
    PrimaryBackupAtomicCounterTest.class,
    RaftAtomicCounterTest.class,
    PrimaryBackupLeaderElectionTest.class,
    RaftLeaderElectionTest.class,
    PrimaryBackupLeaderElectorTest.class,
    RaftLeaderElectorTest.class,
    PrimaryBackupIdGeneratorTest.class,
    RaftIdGeneratorTest.class,
    PrimaryBackupDistributedListTest.class,
    RaftDistributedListTest.class,
    PrimaryBackupAtomicLockTest.class,
    RaftAtomicLockTest.class,
    PrimaryBackupDistributedLockTest.class,
    RaftDistributedLockTest.class,
    PrimaryBackupAtomicCounterMapTest.class,
    RaftAtomicCounterMapTest.class,
    PrimaryBackupAtomicMapTest.class,
    RaftAtomicMapTest.class,
    PrimaryBackupAtomicNavigableMapTest.class,
    RaftAtomicNavigableMapTest.class,
    PrimaryBackupDistributedMapTest.class,
    RaftDistributedMapTest.class,
    PrimaryBackupDistributedNavigableMapTest.class,
    RaftDistributedNavigableMapTest.class,
    PrimaryBackupAtomicMultimapTest.class,
    RaftAtomicMultimapTest.class,
    PrimaryBackupDistributedMultimapTest.class,
    RaftDistributedMultimapTest.class,
    PrimaryBackupDistributedMultisetTest.class,
    RaftDistributedMultisetTest.class,
    PrimaryBackupDistributedQueueTest.class,
    RaftDistributedQueueTest.class,
    PrimaryBackupAtomicSemaphoreTest.class,
    RaftAtomicSemaphoreTest.class,
    PrimaryBackupDistributedSemaphoreTest.class,
    RaftDistributedSemaphoreTest.class,
    AntiEntropyDistributedSetTest.class,
    CrdtDistributedSetTest.class,
    PrimaryBackupDistributedNavigableSetTest.class,
    RaftDistributedNavigableSetTest.class,
    PrimaryBackupDistributedSetTest.class,
    RaftDistributedSetTest.class,
    PrimaryBackupTransactionalMapTest.class,
    RaftTransactionalMapTest.class,
    PrimaryBackupTransactionalSetTest.class,
    RaftTransactionalSetTest.class,
    PrimaryBackupAtomicDocumentTreeTest.class,
    RaftAtomicDocumentTreeTest.class,
    CrdtDistributedValueTest.class,
    PrimaryBackupAtomicValueTest.class,
    RaftAtomicValueTest.class,
    PrimaryBackupWorkQueueTest.class,
    RaftWorkQueueTest.class,
})
public class PrimitivesSuite {
  @ClassRule
  public static PrimitiveResource primitives = PrimitiveResource.getInstance();
}
