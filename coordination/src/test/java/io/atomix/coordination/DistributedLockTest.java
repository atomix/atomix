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

import io.atomix.coordination.state.LockState;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.ResourceStateMachine;
import org.testng.annotations.Test;

/**
 * Async lock test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedLockTest extends AbstractCoordinationTest {

  @Override
  protected ResourceStateMachine createStateMachine() {
    return new LockState();
  }

  /**
   * Tests locking and unlocking a lock.
   */
  public void testLockUnlock() throws Throwable {
    createServers(3);

    DistributedLock lock = new DistributedLock(createClient());

    lock.lock().thenRun(this::resume);
    await();

    lock.unlock().thenRun(this::resume);
    await();
  }

  /**
   * Tests releasing a lock when the client's session is closed.
   */
  public void testReleaseOnClose() throws Throwable {
    createServers(3);

    CopycatClient client1 = createClient();
    CopycatClient client2 = createClient();

    DistributedLock lock1 = new DistributedLock(client1);
    DistributedLock lock2 = new DistributedLock(client2);

    lock1.lock().thenRun(this::resume);
    await();

    lock2.lock().thenRun(this::resume);
    client1.close();
    await();
  }

}
