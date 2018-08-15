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
package io.atomix.core.lock;

import io.atomix.core.Atomix;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Raft distributed lock test.
 */
public class RaftAtomicLockTest extends AtomicLockTest {
  @Override
  protected ProxyProtocol protocol() {
    return MultiRaftProtocol.builder("raft")
        .withMaxRetries(5)
        .build();
  }

  @Test
  public void testDelete() throws Exception {
    Atomix client = atomix();

    AtomicLock lock;
    lock = atomix().atomicLockBuilder("test-" + protocol().group() + "-atomic-lock-delete")
        .withProtocol(protocol())
        .build();

    int count = client.getPrimitives(lock.type()).size();
    lock.delete();
    assertEquals(count - 1, client.getPrimitives(lock.type()).size());

    try {
      lock.isLocked();
      fail();
    } catch (PrimitiveException.ClosedSession e) {
    }

    lock = atomix().atomicLockBuilder("test-" + protocol().group() + "-atomic-lock-delete")
        .withProtocol(protocol())
        .build();
    assertEquals(count, client.getPrimitives(lock.type()).size());
  }
}
