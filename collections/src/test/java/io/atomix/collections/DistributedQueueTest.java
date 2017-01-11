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
 * limitations under the License
 */
package io.atomix.collections;

import io.atomix.testing.AbstractCopycatTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Distributed queue test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class DistributedQueueTest extends AbstractCopycatTest<DistributedQueue> {
  
  @Override
  protected Class<? super DistributedQueue> type() {
    return DistributedQueue.class;
  }

  /**
   * Tests offering an item to a queue and then polling it.
   */
  public void testQueueOfferPoll() throws Throwable {
    createServers(3);

    DistributedQueue<String> queue1 = createResource();
    DistributedQueue<String> queue2 = createResource();

    queue1.offer("Hello world!").join();
    queue2.size().thenAccept(size -> {
      threadAssertEquals(1, size);
      resume();
    });
    await(10000);

    queue2.poll().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await(10000);

    queue2.isEmpty().thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests offering an item to a queue and then removing it.
   */
  public void testQueueOfferRemove() throws Throwable {
    createServers(3);

    DistributedQueue<String> queue1 = createResource();
    DistributedQueue<String> queue2 = createResource();

    queue1.offer("Hello world!").join();
    queue2.size().thenAccept(size -> {
      threadAssertEquals(1, size);
      resume();
    });
    await(10000);

    queue2.remove().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await(10000);

    queue2.isEmpty().thenAccept(result -> {
      threadAssertTrue(result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests offering an item to a queue and then peeking at it.
   */
  public void testQueueOfferPeek() throws Throwable {
    createServers(3);

    DistributedQueue<String> queue1 = createResource();
    DistributedQueue<String> queue2 = createResource();

    queue1.offer("Hello world!").join();
    queue2.size().thenAccept(size -> {
      threadAssertEquals(1, size);
      resume();
    });
    await(10000);

    queue2.peek().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await(10000);

    queue2.isEmpty().thenAccept(result -> {
      threadAssertFalse(result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests offering an item to a queue and then getting the first element from it.
   */
  public void testQueueOfferElement() throws Throwable {
    createServers(3);

    DistributedQueue<String> queue1 = createResource();
    DistributedQueue<String> queue2 = createResource();

    queue1.offer("Hello world!").join();
    queue2.size().thenAccept(size -> {
      threadAssertEquals(1, size);
      resume();
    });
    await(10000);

    queue2.element().thenAccept(result -> {
      threadAssertEquals("Hello world!", result);
      resume();
    });
    await(10000);

    queue2.isEmpty().thenAccept(result -> {
      threadAssertFalse(result);
      resume();
    });
    await(10000);
  }

  /**
   * Tests adding and removing members from a queue.
   */
  public void testQueueAddRemove() throws Throwable {
    createServers(3);

    DistributedQueue<String> queue1 = createResource();
    assertFalse(queue1.contains("Hello world!").get());

    DistributedQueue<String> queue2 = createResource();
    assertFalse(queue2.contains("Hello world!").get());

    queue1.add("Hello world!").join();
    assertTrue(queue1.contains("Hello world!").get());
    assertTrue(queue2.contains("Hello world!").get());

    queue2.remove("Hello world!").join();
    assertFalse(queue1.contains("Hello world!").get());
    assertFalse(queue2.contains("Hello world!").get());
  }

  /**
   * Tests various queue events.
   */
  public void testQueueEvents() throws Throwable {
    createServers(3);

    DistributedQueue<String> queue1 = createResource();
    DistributedQueue<String> queue2 = createResource();

    queue1.onAdd(event -> {
      if (event.value().equals("Hello world!")) {
        resume();
      }
    }).thenRun(this::resume);
    await(5000);

    queue1.onAdd(event -> {
      if (event.value().equals("Hello world again!")) {
        resume();
      }
    }).thenRun(this::resume);
    await(5000);

    queue1.onRemove(event -> {
      if (event.value().equals("Hello world!")) {
        resume();
      }
    }).thenRun(this::resume);
    await(5000);

    queue1.onRemove(event -> {
      if (event.value().equals("Hello world again!")) {
        resume();
      }
    }).thenRun(this::resume);
    await(5000);

    queue2.add("Hello world!").thenRun(this::resume);
    await(5000, 2);

    queue2.offer("Hello world again!").thenRun(this::resume);
    await(5000, 2);

    queue2.remove().thenRun(this::resume);
    await(5000, 2);

    queue2.poll().thenRun(this::resume);
    await(5000, 2);
  }

}
