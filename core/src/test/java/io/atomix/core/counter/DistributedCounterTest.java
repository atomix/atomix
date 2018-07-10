/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.core.counter;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.core.counter.impl.AtomicCounterProxy;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link DistributedCounter}.
 */
public abstract class DistributedCounterTest extends AbstractPrimitiveTest {
  @Test
  public void testBasicOperations() throws Throwable {
    DistributedCounter counter1 = atomix().counterBuilder("testBasicOperations", protocol()).build();
    DistributedCounter counter2 = atomix().counterBuilder("testBasicOperations", protocol()).build();

    assertEquals(1, counter1.incrementAndGet());
    assertEquals(2, counter1.incrementAndGet());
    assertEquals(2, counter1.get());

    assertTrue(waitFor(counter2, 2, 5000));
    assertEquals(3, counter2.incrementAndGet());

    assertTrue(waitFor(counter1, 3, 5000));
    assertEquals(4, counter1.incrementAndGet());
    assertEquals(4, counter2.incrementAndGet());

    assertTrue(waitFor(counter1, 5, 5000));
    assertTrue(waitFor(counter2, 5, 5000));
  }

  private boolean waitFor(DistributedCounter counter, long value, long timeout) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    while (true) {
      if (counter.get() == value) {
        return true;
      } else if (System.currentTimeMillis() - startTime > timeout) {
        return false;
      } else {
        Thread.sleep(10);
      }
    }
  }
}
