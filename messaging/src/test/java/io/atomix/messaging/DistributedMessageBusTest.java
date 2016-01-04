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
package io.atomix.messaging;

import io.atomix.catalyst.transport.Address;
import io.atomix.resource.ResourceType;
import io.atomix.testing.AbstractCopycatTest;
import org.testng.annotations.Test;

/**
 * Distributed message bus test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class DistributedMessageBusTest extends AbstractCopycatTest<DistributedMessageBus> {

  @Override
  protected Class<? super DistributedMessageBus> type() {
    return DistributedMessageBus.class;
  }

  /**
   * Tests sending a message.
   */
  public void testSend() throws Throwable {
    createServers(3);

    DistributedMessageBus bus1 = createResource();
    DistributedMessageBus bus2 = createResource();

    bus1.open(new Address("localhost", 6000)).join();
    bus2.open(new Address("localhost", 6001)).join();

    bus1.<String>consumer("test", message -> {
      threadAssertEquals(message, "Hello world!");
      resume();
      return null;
    }).thenRun(() -> {
      bus2.producer("test").thenAccept(producer -> {
        producer.send("Hello world!");
      });
    });

    await(10000);
  }

}
