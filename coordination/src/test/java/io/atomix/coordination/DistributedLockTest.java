/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.coordination;

import io.atomix.testing.AbstractCopycatTest;
import org.testng.annotations.Test;

import java.time.Duration;

/**
 * Async lock test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedLockTest extends AbstractCopycatTest<DistributedLock> {
  
  @Override
  protected Class<? super DistributedLock> type() {
    return DistributedLock.class;
  }

  /**
   * Tests locking and unlocking a lock.
   */
  public void testLockUnlock() throws Throwable {
    createServers(3);

    DistributedLock lock = createResource();

    lock.lock().thenRun(this::resume);
    await(10000);

    lock.unlock().thenRun(this::resume);
    await(10000);
  }

  /**
   * Tests releasing a lock when the client's session is closed.
   */
  public void testReleaseOnClose() throws Throwable {
    createServers(3);

    DistributedLock lock1 = createResource();
    DistributedLock lock2 = createResource();

    lock1.lock().thenRun(this::resume);
    await(10000);

    lock2.lock().thenRun(this::resume);
    lock1.close();
    await(10000);
  }

  /**
   * Tests attempting to acquire a lock with a timeout.
   */
  public void testTryLockFail() throws Throwable {
    createServers(3);

    DistributedLock lock1 = createResource();
    DistributedLock lock2 = createResource();

    lock1.lock().thenRun(this::resume);
    await(10000);

    lock2.tryLock(Duration.ofSeconds(1)).thenAccept(result -> {
      threadAssertNull(result);
      resume();
    });
    await(10000);
  }

}
